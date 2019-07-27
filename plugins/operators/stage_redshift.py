from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
                COPY {}
                FROM '{}'
                CREDENTIALS 'aws_iam_role={}'
                FORMAT AS JSON '{}'
            """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 create_table_sql="",
                 aws_iam_role_key="",
                 json_format="",
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.create_table_sql = create_table_sql

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("DROP TABLE from Redshift")
	redshift.run("DROP TABLE IF EXISTS {}".format(self.table))

        self.log.info("CREATE TABLE in Redshift")
	redshift.run(self.create_table_sql)

    copy_sql = """
                COPY {}
                FROM '{}'
                CREDENTIALS 'aws_iam_role={}'
                FORMAT AS JSON '{}'
            """

        self.log.info("Copying to redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key
        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.aws_iam_role_key,
            self.json_format
        )
        redshift.run(formatted_sql)






