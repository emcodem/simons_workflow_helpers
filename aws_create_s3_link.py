import os
import sys
import argparse
import boto3
import textwrap
from botocore.config import Config

def check_env_vars(vars_list):
    """Ensure AWS credentials exist in the environment."""
    missing = [v for v in vars_list if not os.getenv(v)]
    if missing:
        print(f"\n❌ Error: Missing environment variables: {', '.join(missing)}")
        print("Run 'python your_script.py --help' to see how to set them.\n")
        sys.exit(1)

def main():
    description = "Generate a pre-signed S3 PUT URL for secure file uploads."
    
    # Updated epilog with CMD and PowerShell examples
    epilog = textwrap.dedent(f'''
        Environment Variables Setup:
        ---------------------------
        [Mac / Linux / WSL]
          export AWS_ACCESS_KEY_ID='AKIA...'
          export AWS_SECRET_ACCESS_KEY='secret...'
          export AWS_REGION='eu-central-1'
        
        [Windows PowerShell]
          $env:AWS_ACCESS_KEY_ID="AKIA..."
          $env:AWS_SECRET_ACCESS_KEY="secret..."
          $env:AWS_REGION="eu-central-1"

        [Windows Command Prompt (CMD)]
          set AWS_ACCESS_KEY_ID=AKIA...
          set AWS_SECRET_ACCESS_KEY=secret...
          set AWS_REGION=eu-central-1

        Usage Example:
        --------------
        python {sys.argv[0]} --bucket my-ingest-bucket --file video.mxf --expiry 7200
    ''')

    parser = argparse.ArgumentParser(
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("--bucket", required=True, help="The target S3 bucket name")
    parser.add_argument("--file", required=True, help="The name the file will have in S3")
    parser.add_argument("--expiry", type=int, default=3600, help="URL lifetime in seconds (default: 3600)")
    
    args = parser.parse_args()

    # Verify credentials before proceeding
    check_env_vars(['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION'])

    try:
        s3 = boto3.client(
            's3',
            region_name=os.getenv('AWS_REGION'),
            config=Config(signature_version='s3v4', s3={'addressing_style': 'virtual'})
        )

        url = s3.generate_presigned_url(
            ClientMethod='put_object',
            Params={'Bucket': args.bucket, 'Key': args.file},
            ExpiresIn=args.expiry
        )

        print(f"\n✅ Presigned URL Generated:\n{url}\n")

    except Exception as e:
        print(f"❌ AWS Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()