import { S3Client, PutObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';

export class S3Service {
  private s3: S3Client;
  private bucketName: string;

  constructor() {
    const region = process.env.AWS_REGION;
    this.bucketName = process.env.S3_BUCKET_NAME!;

    if (!region || !this.bucketName) {
      throw new Error('AWS_REGION and S3_BUCKET_NAME environment variables must be set.');
    }

    this.s3 = new S3Client({ region });
  }

  /**
   * Checks if a file exists in S3 at the given key.
   * Optimized to avoid scanning the bucket.
   */
  async checkFileExists(key: string): Promise<boolean> {
    try {
      const command = new HeadObjectCommand({
        Bucket: this.bucketName,
        Key: key,
      });
      await this.s3.send(command);
      return true;
    } catch (error: any) {
      if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Uploads a file to the S3 bucket.
   * @param key The full S3 key (path) for the destination file.
   * @param body The content of the file.
   */
  async uploadFile(key: string, body: string): Promise<void> {
    const command = new PutObjectCommand({
      Bucket: this.bucketName,
      Key: key,
      Body: body,
      ContentType: 'application/json',
    });

    await this.s3.send(command);
    console.log(`Successfully uploaded to S3: s3://${this.bucketName}/${key}`);
  }
}
