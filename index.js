

require('dotenv').config();
const util = require('util');
const exec = util.promisify(require('child_process').exec);
const s3 = require("@aws-sdk/client-s3");
const fs = require('fs');
const cron = require("cron");
const {ListObjectsV2Command, DeleteObjectsCommand} = require("@aws-sdk/client-s3");

function loadConfig() {
  const requiredEnvars = [
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_S3_REGION',
    'AWS_S3_ENDPOINT',
    'AWS_S3_BUCKET',
    'BACKUP_RETENTION_DAYS',
    'BACKUP_RETENTION_WEEKS',
    'BACKUP_RETENTION_MONTHS'
  ];
  
  for (const key of requiredEnvars) {
    if (!process.env[key]) {
      throw new Error(`Environment variable ${key} is required`);
    }
  }

  return {
    aws: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      region: process.env.AWS_S3_REGION,
      endpoint: process.env.AWS_S3_ENDPOINT,
      s3_bucket: process.env.AWS_S3_BUCKET
    },
    databases: process.env.DATABASES ? process.env.DATABASES.split(",") : [],
    run_on_startup: process.env.RUN_ON_STARTUP === 'true',
    cron: process.env.CRON,
    retention: {
       days:  process.env.BACKUP_RETENTION_DAYS,
       weeks:  process.env.BACKUP_RETENTION_WEEKS,
       months:  process.env.BACKUP_RETENTION_MONTHS,
    }
  };
}

const config = loadConfig();

// noinspection JSCheckFunctionSignatures
const s3Client = new s3.S3Client(config.aws);

async function processBackup() {
  if (config.databases.length === 0) {
    console.log("No databases defined.");
    return;
  }

  for (const [index, databaseURI] of config.databases.entries()) {
    const databaseIteration = index + 1;
    const totalDatabases = config.databases.length;

    const url = new URL(databaseURI);
    const dbType = url.protocol.slice(0, -1); // remove trailing colon
    const dbName = url.pathname.substring(1); // extract db name from URL
    const dbHostname = url.hostname;
    const dbUser = url.username;
    const dbPassword = url.password;
    const dbPort = url.port;
  
    const date = new Date();
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    const hh = String(date.getHours()).padStart(2, '0');
    const min = String(date.getMinutes()).padStart(2, '0');
    const ss = String(date.getSeconds()).padStart(2, '0');
    const timestamp = `${yyyy}-${mm}-${dd}_${hh}:${min}:${ss}`;
    const filename = `backup-${dbType}-${timestamp}-${dbName}-${dbHostname}.tar.gz`;
    const filepath = `/tmp/${filename}`;

    console.log(`\n[${databaseIteration}/${totalDatabases}] ${dbType}/${dbName} Backup in progress...`);

    let dumpCommand;
    let versionCommand = 'echo "Unknown database type"';
    switch (dbType) {
      case 'postgresql':
        dumpCommand = `pg_dump "${databaseURI}" -F c > "${filepath}.dump"`;
        versionCommand = 'psql --version';
        break;
      case 'mongodb':
        dumpCommand = `mongodump --uri="${databaseURI}" --archive="${filepath}.dump"`;
        versionCommand = 'mongodump --version';
        break;
      case 'mysql':
        dumpCommand = `mysqldump -u ${dbUser} -p${dbPassword} -h ${dbHostname} -P ${dbPort} ${dbName} > "${filepath}.dump"`;
        versionCommand = 'mysql --version';
        break;
      default:
        console.log(`Unknown database type: ${dbType}`);
        return;
    }

    try {
      // Log database client version
      try {
        const { stdout: versionOutput } = await exec(versionCommand);
        console.log(`Using ${dbType} client version:`, versionOutput.trim());
      } catch (versionError) {
        console.warn(`Failed to get ${dbType} client version:`, versionError.message);
      }

      // 1. Execute the dump command
      await exec(dumpCommand);

      // 2. Compress the dump file
      await exec(`tar -czvf ${filepath} ${filepath}.dump`);

      // 3. Read the compressed file
      const data = fs.readFileSync(filepath);

      // 4. Upload to S3
      const params = {
        Bucket: config.aws.s3_bucket,
        Key: filename,
        Body: data
      };

      const putCommand = new s3.PutObjectCommand(params);
      await s3Client.send(putCommand);
      
      console.log(`✓ Successfully uploaded db backup for database ${dbType} ${dbName} ${dbHostname}.`);

      // 5. Clean up temporary files
      await exec(`rm -f ${filepath} ${filepath}.dump`);
    } catch (error) {
      console.error(`An error occurred while processing the database ${dbType} ${dbName}, host: ${dbHostname}): ${error}`,error);
    }
  }
}

async function cleanupOldBackups() {
  console.log("🧹 Cleaning up old backups...");

  // 1. List all backups
  let objects = [];
  let continuationToken = null;

  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: config.aws.s3_bucket,
      ContinuationToken: continuationToken,
    }));
    if (response.Contents) {
      objects = objects.concat(response.Contents);
    }
    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  // 2. Sort by date (newest first)
  objects.sort((a, b) => (b.LastModified?.getTime() || 0) - (a.LastModified?.getTime() || 0));

  const toDelete = [];
  const now = new Date();

  // 3. Define Retention Cutoffs
  const dailyCutoff = new Date(now.getTime() - config.retention.days * 24 * 60 * 60 * 1000);
  const weeklyCutoff = new Date(now.getTime() - config.retention.weeks * 7 * 24 * 60 * 60 * 1000);
  const monthlyCutoff = new Date(now.getTime() - config.retention.months * 30 * 24 * 60 * 60 * 1000);

  // 4. Filter objects to keep or delete
  for (const obj of objects) {
    const key = obj.Key;
    if (!key) continue;

    const date = obj.LastModified;
    if (!date) continue;

    const isSunday = date.getDay() === 0;
    const isFirstOfMonth = date.getDate() === 1;

    let keep = false;

    if (date > dailyCutoff) keep = true;
    else if (date > weeklyCutoff && isSunday) keep = true;
    else if (date > monthlyCutoff && isFirstOfMonth) keep = true;

    if (!keep) toDelete.push(key);
  }

  // 5. Delete in batches
  if (toDelete.length > 0) {
    console.log(`🗑️ Deleting ${toDelete.length} old backups...`);
    for (let i = 0; i < toDelete.length; i += 1000) {
      const batch = toDelete.slice(i, i + 1000).map(key => ({ Key: key }));
      await s3Client.send(new DeleteObjectsCommand({
        Bucket: config.aws.s3_bucket,
        Delete: { Objects: batch },
      }));
    }
    console.log("✅ Cleanup completed.");
  } else {
    console.log("✨ No backups to clean up.");
  }
}

if (config.cron) {
  const CronJob = cron.CronJob;
  const job = new CronJob(config.cron, processBackup);
  job.start();
  
  console.log(`Backups configured on Cron job schedule: ${config.cron}`);
}

if (config.run_on_startup) {
  console.log("run_on_startup enabled, backing up now...")
  processBackup().then(() => console.log("added backup"));

  cleanupOldBackups().then(() => console.log("cleaned up backups"))
}