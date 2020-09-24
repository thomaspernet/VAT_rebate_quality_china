# Documentation

Add any documentation useful for the project here



## Basic IAM policy

### S3

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::BUCKETNAME",
        "arn:aws:s3:::BUCKETNAME"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:PutObjectAcl",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::BUCKETNAME/*",
        "arn:aws:s3:::BUCKETNAME/*"
      ]
    }
  ]
}
```



## Athena

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VisualEditor0",
      "Effect": "Allow",
      "Action": [
        "glue:BatchCreatePartition",
        "glue:UpdateDatabase",
        "s3:ListBucketMultipartUploads",
        "glue:CreateTable",
        "glue:DeleteDatabase",
        "glue:GetTables",
        "glue:GetPartitions",
        "glue:BatchDeletePartition",
        "glue:UpdateTable",
        "glue:BatchGetPartition",
        "glue:DeleteTable",
        "s3:CreateBucket",
        "s3:ListBucket",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetDatabase",
        "glue:GetPartition",
        "glue:CreateDatabase",
        "glue:BatchDeleteTable",
        "glue:CreatePartition",
        "s3:DeleteObject",
        "glue:DeletePartition",
        "s3:GetBucketLocation",
        "glue:UpdatePartition"
      ],
      "Resource": [
        "arn:aws:glue:*:*:table/*/*",
        "arn:aws:glue:*:*:database/*",
        "arn:aws:glue:*:*:catalog",
        "arn:aws:s3:::BUCKETNAME/*",
        "arn:aws:s3:::BUCKETNAME"
      ]
    },
    {
      "Sid": "VisualEditor1",
      "Effect": "Allow",
      "Action": [
        "glue:BatchCreatePartition",
        "glue:UpdateDatabase",
        "glue:CreateTable",
        "glue:DeleteDatabase",
        "glue:GetTables",
        "glue:GetPartitions",
        "glue:BatchDeletePartition",
        "glue:UpdateTable",
        "glue:BatchGetPartition",
        "glue:DeleteTable",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetDatabase",
        "glue:GetPartition",
        "glue:CreateDatabase",
        "glue:BatchDeleteTable",
        "glue:CreatePartition",
        "glue:DeletePartition",
        "glue:UpdatePartition"
      ],
      "Resource": [
        "arn:aws:glue:REGION:USERID:table/*/*",
        "arn:aws:glue:REGION:USERID:database/*"
      ]
    },
    {
      "Sid": "VisualEditor2",
      "Effect": "Allow",
      "Action": [
        "glue:BatchCreatePartition",
        "glue:UpdateDatabase",
        "glue:CreateTable",
        "glue:DeleteDatabase",
        "glue:GetTables",
        "glue:GetPartitions",
        "glue:BatchDeletePartition",
        "glue:UpdateTable",
        "glue:BatchGetPartition",
        "glue:DeleteTable",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetDatabase",
        "glue:GetPartition",
        "glue:CreateDatabase",
        "glue:BatchDeleteTable",
        "glue:CreatePartition",
        "glue:DeletePartition",
        "glue:UpdatePartition"
      ],
      "Resource": [
        "arn:aws:glue:REGION:USERID:catalog/*",
        "arn:aws:glue:*:*:table/*/*",
        "arn:aws:glue:*:*:database/*"
      ]
    },
    {
      "Sid": "VisualEditor3",
      "Effect": "Allow",
      "Action": "athena:*",
      "Resource": "arn:aws:athena:REGION:USERID:workgroup/primary"
    }
  ]
}
```

