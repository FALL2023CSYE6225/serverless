const Mailgun = require('mailgun.js');
const formData = require('form-data');
const axios = require('axios');
const { Storage } = require('@google-cloud/storage');
const JSZip = require('jszip');
const fs = require('fs');
const AWS = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');

const mailgun = new Mailgun(formData);

const counts = {};

// Function to generate a unique local and destination object name using a counter
function generateFileNames(name) {
  if (!counts[name]) {
    counts[name] = 1;
  }
  let counter = counts[name]; // Get current counter for name
  let uniqueFileName = `${name}_${counter}.zip`;
  counts[name]++; // Increment counter for the name
  return {
    localPath: `/tmp/${uniqueFileName}`,
    destinationBlobName: `${name}/${uniqueFileName}`,
  };
}

async function isZipEmpty(zipPath) {
  const data = fs.readFileSync(zipPath);
  const zip = await JSZip.loadAsync(data);
  const fileNames = Object.keys(zip.files);

  for (let fileName of fileNames) {
    const fileData = await zip.files[fileName].async('nodebuffer');
    if (fileData.length > 0) {
      return false;
    }
  }

  return true;
}

const mg = mailgun.client({
  username: 'api',
  key: process.env.MAILGUN_API_KEY,
});
async function uploadToGCP(
  bucketName,
  sourceFilePath,
  destinationBlobName,
  credentials
) {
  const storage = new Storage({
    credentials: credentials,
    projectId: credentials.project_id,
  });

  // Reference the bucket
  const bucket = storage.bucket(bucketName);

  // Upload the file to GCP Storage
  await bucket.upload(sourceFilePath, {
    destination: destinationBlobName,
  });

  return destinationBlobName;
}

// Function to download a file from a URL and save it locally
async function downloadFile(url, localPath) {
  const response = await axios({
    url,
    method: 'GET',
    responseType: 'stream',
  });

  return new Promise((resolve, reject) => {
    const fileStream = fs.createWriteStream(localPath);
    response.data.pipe(fileStream);
    fileStream.on('finish', () => resolve(true));
    fileStream.on('error', (error) => reject(error));
  });
}

async function sendMail(name, email, emailSubject, emailBody) {
  try {
    await mg.messages.create('ajitpatil.me', {
      from: "Ajit Patil <no-reply@ajitpatil.me>",
      to: [email],
      subject: emailSubject,
      text: `Hello ${name},\n\n `,
      html: `<h3>Hello ${name},</h3><p>${emailBody}</p><p>Best Regards,</p><p>Ajit Patil</p>`,
    });
  } catch (error) {
    console.error('Error sending mail:', error);
  }
}

// Function to Log Email Event in DynamoDB
async function logEmailEvent(
  dynamoDB,
  tableName,
  name,
  email,
  eventType,
  details
) {
  const timestamp = new Date().toISOString();
  const eventId = uuidv4();

  const params = {
    TableName: tableName,
    Item: {
      ID: eventId,
      Name: name,
      Email: email,
      Timestamp: timestamp,
      Status: eventType,
      StatusDetails: details,
    },
  };

  await dynamoDB.put(params).promise();
}

const handlerLambda = async (event) => {
  let dynamoDB = new AWS.DynamoDB.DocumentClient(); // DynamoDB Client
  let tableName = process.env.DYNAMODB_TABLE_NAME; // Taking DynamoDB Table Name from Lambda's Environment Variables

  const message = JSON.parse(event.Records[0].Sns.Message);
  const { name, url, email } = message;
  console.log(name);
  console.log(url);
  console.log(email);
  const bucketName = process.env.GCP_BUCKET_NAME;
  const gcpCredentialsBase64 = process.env.GCP_CREDENTIALS;
  if (!(bucketName && gcpCredentialsBase64 && name && url)) {
    throw new Error('Missing required data');
  }

  const gcpCredentialsJson = Buffer.from(
    gcpCredentialsBase64,
    'base64'
  ).toString('utf-8');
  const credentials = JSON.parse(gcpCredentialsJson);
  const { localPath, destinationBlobName } = generateFileNames(name);
  console.log('local path', localPath);
  console.log('destination path', destinationBlobName);

  try {
    const downloadSuccess = await downloadFile(url, localPath);
    console.log('downloadSuccess', downloadSuccess);
    if (downloadSuccess) {
      //console.log('inside block');
      const isZipContentEmpty = await isZipEmpty(localPath);
      console.log('isZipEmtpy', isZipContentEmpty);
      if (isZipContentEmpty == false) {
        console.log('Files not zero bytes');
        await uploadToGCP(
          bucketName,
          localPath,
          destinationBlobName,
          credentials
        );
        const emailSubject = 'Assignment Download and Upload Successful';
        const emailBody = `The Assignment files has been successfully downloaded and uploaded to GCP Bucket having a destination path: gs://${bucketName}/${destinationBlobName}`;
        await sendMail(name, email, emailSubject, emailBody);
        await logEmailEvent(
          dynamoDB,
          tableName,
          name,
          email,
          'Success',
          'File downloaded and uploaded successfully'
        );
      } else if (isZipContentEmpty == true) {
        console.log('Files  zero bytes');
        throw new Error('Empty File');
      }
    }
  } catch (error) {
    //console.error(`Error in processing: ${error}`);
    let emailSubject, emailBody;
    if (error.message === 'Empty File') {
      emailSubject = 'Empty Assignment Contents Downloaded';
      emailBody = `Your assignment files in zip,appears to be empty. Please check and resubmit a valid zip file.`;
    } else {
      emailSubject = 'Assignment Download Failed Due to Invalid URL';
      emailBody = `There was an error downloading your assignment files with the given submission url, please check the URL and resubmit.`;
    }
    await sendMail(name, email, emailSubject, emailBody);
    await logEmailEvent(
      dynamoDB,
      tableName,
      name,
      email,
      'Failure',
      error.message
    );
  }
};

exports.handler = handlerLambda;
