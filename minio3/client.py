import datetime
import logging
import os
from abc import ABC
from io import BytesIO, StringIO
import json
import pandas as pd
import awswrangler as wr
from boto3 import Session
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'minio:9000')
DATAFRAME_FILETYPES = ['csv', 'json', 'parquet']


class Minio3(ABC):
    def __init__(
            self,
            region='us-east-1',
            verbose=1,
            refresh_day_shift=7,
            secure=False,
    ):
        """
        Initialize Minio3 object with connection parameters.
        Handles crud operations for:
        - Dateless Files: /test.json | /test.html | test.csv | etc...
        - Date Files: /20230718.json | /20230718.html | 20230718.csv | etc...
        - Dataset Files: /partition_year=2023 (parquet)

        Parameters:
            region (str): AWS region name. Default is 'us-east-1'.
            verbose (int): Verbosity level for logging. Default is 1.
            refresh_day_shift (int): Number of days to subtract from last modified date. Default is 7.
            secure (bool): Set to True for HTTPS connections, False for HTTP. Default is False.
        """
        self.region = region
        self._verbose = verbose
        self.refresh_day_shift = refresh_day_shift
        self._secure = secure
        self._minio_endpoint_url = f"https://{MINIO_ENDPOINT}" if self._secure else f"http://{MINIO_ENDPOINT}"
        Session.client.defaults = (None, None, self._secure, None, self._minio_endpoint_url, None, None, None, None)
        wr.config.s3_endpoint_url = self._minio_endpoint_url
        self.session = Session(
            aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY'),
            aws_session_token=None,
            region_name=self.region,
            botocore_session=None,
            profile_name=None
        )

        self._client = self.session.client(
            's3',
            aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY'),
            endpoint_url=self._minio_endpoint_url
        )
        self._resource = self.session.resource(
            's3',
            aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY'),
            endpoint_url=self._minio_endpoint_url
        )

    def get_paths(self, root_path, subpaths=None):
        """
        Get a list of S3 file paths within the specified root path and subpaths.

        This method retrieves a list of S3 file paths within the specified root_path and
        optionally specified subpaths. It lists all the files in the given root path and
        its subdirectories, returning their corresponding S3 paths.

        Args:
            root_path (str): The S3 path to the root directory, e.g., 's3://bucket-name/root/'.
            subpaths (list, optional): A list of subdirectories within the root_path to search for files.
                                      If not provided, it will list files directly under the root_path.

        Returns:
            list: A list of S3 file paths found within the specified root_path and subpaths.
                  If no files are found or an error occurs, it returns an empty list.
        """
        files = []
        subpaths = [''] if subpaths is None else subpaths

        # Decompose the root_path into its components: path, bucket_name, key, file_name, file_format
        path, bucket_name, key, file_name, file_format = _filter_s3_path(root_path)

        try:
            # Loop through each sub_path and list files within them
            for sub_path in subpaths:
                if sub_path != '':
                    filter_key = f"{key}/{sub_path}"
                else:
                    filter_key = key

                # Get the initial list of files
                file_list_res = self._client.list_objects_v2(Bucket=bucket_name, Prefix=filter_key, MaxKeys=10000)
                files.extend([file['Key'] for file in file_list_res['Contents']])

                # If more files exist, continue listing using continuation tokens
                while 'NextContinuationToken' in file_list_res:
                    res = self._client.list_objects_v2(Bucket=bucket_name, Prefix=filter_key, MaxKeys=10000, ContinuationToken=file_list_res['NextContinuationToken'])
                    files.extend([file['Key'] for file in res['Contents']])

            # Return the list of S3 file paths with the 's3://' prefix
            return [f"s3://{bucket_name}/{file}" for file in files]

        except KeyError as e:
            # If KeyError occurs, return the list of S3 file paths with the 's3://' prefix
            return [f"s3://{bucket_name}/{file}" for file in files]

        except Exception as e:
            # If any other exception occurs, log the error and return an empty list
            logger.error(f'Error getting paths for {root_path}', exc_info=e)
            return []

    def needs_update(self, root_path, subpaths=None, update_after=None):
        """
        Check for files that need updating in the specified S3 path.

        This method retrieves a list of files within the specified S3 'root_path' and its 'subpaths' (if provided),
        and determines if any of these files need updating based on the 'update_after' timestamp.
        Args:
            root_path (str): The root S3 path to search for files that may need updating.
            subpaths (list, optional): A list of subdirectories to search for files within the 'root_path'. If not provided, the method searches only the 'root_path'.
            update_after (str or datetime, optional): A timestamp or datetime indicating the minimum last modified date for files to be considered for an update. If None, the default refresh day shift will be used.

        Returns:
            list: A list of S3 file paths that need updating, i.e., files with last modified dates greater than or equal to the 'update_after' timestamp.
        """
        try:
            # Decompose the root_path into its components: path, bucket_name, key, file_name, file_format
            path, bucket_name, key, file_name, file_format = _filter_s3_path(root_path)
            paths_to_update = []
            files = self.get_paths(root_path, subpaths)
            for file in files:
                date = pd.Timestamp(file['Key'].replace(key, '').split('.')[0])
                if update_after is not None:
                    last_modified = pd.Timestamp(update_after)
                else:
                    last_modified = pd.Timestamp(file['LastModified'].date() - datetime.timedelta(days=self.refresh_day_shift))
                if date >= last_modified:
                    paths_to_update.append(f"s3://{bucket_name}/{file['Key']}")
            return paths_to_update
        except Exception as e:
            return []

    def file_exists(self, root_path):
        """
        Check if the dataset file exists in the specified path.

        Args:
            root_path (str): The path to the dataset file in the format 's3://bucket-name/path/to/file'.

        Returns:
            bool: True if the dataset file exists, False otherwise.
        """
        try:
            path, bucket_name, key, file_name, file_format = _filter_s3_path(root_path)
            bucket_obj = self._client.list_objects_v2(Bucket=bucket_name, Prefix=key, MaxKeys=1)
            res = bucket_obj['Contents'][0]
            return True
        except Exception as e:
            return False

    def get_last_update(self, path, date_column, valid_filter=None, cols=None):
        """
        Get the last update date for a dataset file in the specified path.

        Args:
            path (str): The path to the dataset file in the format 's3://bucket-name/path/to/file'.
            date_column (str): Name of the column containing date information for filtering.
            valid_filter (str): A filter expression to apply on the dataset.
            cols (List[str]): List of columns to retrieve from the dataset.

        Returns:
            datetime.date: The last update date of the dataset.
        """
        try:
            if cols is None:
                data_cols = [date_column, 'lastupdated']
            else:
                if 'lastupdated' not in cols:
                    cols = cols + ['lastupdated']
                data_cols = cols

            files = []
            for year in list(range(datetime.datetime.now().year, 1984, -1)):
                files = self.get_paths(path, subpaths=[year])
                if files != []:
                    # Force check previous year update
                    files.extend(self.get_paths(path, subpaths=[year - 1]))
                    break
            if files == []:
                return datetime.date(1900, 1, 1)
            else:
                sorted(files, reverse=True)

            s3_df = pd.concat([self.get_file(root_path=file, cols=data_cols) for file in files])
            if valid_filter is not None:
                filtered_df = s3_df.query(valid_filter)
                if filtered_df.shape[0] != 0:
                    s3_df = filtered_df
            last_update = s3_df.sort_values('datetime', ascending=False)['datetime'].values[0]
            last_update = pd.Timestamp(pd.Timestamp(last_update) - datetime.timedelta(days=self.refresh_day_shift)).to_pydatetime()
            last_update_str = last_update.strftime('%Y/%m/%d')
            return last_update
        except Exception as e:
            logger.error(f"Error get_last_update for {path} defaulting to 1900-01-01 ", exc_info=e)
            return datetime.date(1900, 1, 1)

    def get_last_dataset_update(self, path, date_column, valid_filter=None, cols=None):
        """
        Get the last update date from a dataset stored in an S3 path.

        This method retrieves the last update date from the dataset files
        stored in the specified S3 path. It searches for the latest files
        in the dataset using the provided date_column and optional valid_filter.
        The dataset files are assumed to be organized by partitions based on the
        'partition_year' field in the dataset.

        Args:
            path (str): The S3 path to the dataset directory, e.g., 's3://bucket-name/dataset/'.
            date_column (str): The name of the column that contains date information for updates.
            valid_filter (str, optional): An optional filter expression to select specific rows based on conditions.
            cols (list, optional): A list of column names to be included in the retrieved data.

        Returns:
            datetime.datetime or datetime.date: The last update date from the dataset.
                If the dataset is empty or an error occurs, it returns the default date '1900-01-01'.
        """
        try:
            if cols is None:
                data_cols = [date_column, 'lastupdated']
            else:
                if 'lastupdated' not in cols:
                    cols = cols + ['lastupdated']
                data_cols = cols

            df = pd.DataFrame()
            for year in list(range(datetime.datetime.now().year, 1984, -1)):
                s3_df = self.get_dataset_file(
                    root_path=path,
                    partition_filter=lambda x: str(x['partition_year']) == str(year),
                    cols=data_cols
                )
                if s3_df.shape[0] > 0:
                    prev_year_s3_df = self.get_dataset_file(
                        root_path=path,
                        partition_filter=lambda x: str(x['partition_year']) == str(year - 1),
                        cols=data_cols
                    )
                    df = pd.concat([s3_df, prev_year_s3_df], ignore_index=True)
                    break

            if df.shape[0] == 0:
                return datetime.date(1900, 1, 1)

            if valid_filter is not None:
                filtered_df = df.query(valid_filter)
                if filtered_df.shape[0] != 0:
                    df = filtered_df
                else:
                    return datetime.date(1900, 1, 1)
            if 'datetime' not in df.columns:
                df['datetime'] = pd.to_datetime(df['date'])
            last_update = df.sort_values('datetime', ascending=False)['datetime'].values[0]
            last_update = pd.Timestamp(pd.Timestamp(last_update) - datetime.timedelta(days=self.refresh_day_shift)).to_pydatetime()
            last_update_str = last_update.strftime('%Y-%m-%d')
            return last_update
        except Exception as e:
            logger.error(f"Error get_last_update for {path} defaulting to 1900-01-01 ", exc_info=e)
            return datetime.date(1900, 1, 1)

    def get_file(self, root_path, cols=None, lake_dateless_prefix='datalake-raw/raw'):
        try:
            sep = _get_sep(root_path=root_path, lake_dateless_prefix=lake_dateless_prefix)
            path, bucket_name, key, file_name, file_format = _filter_s3_path(root_path)
            if file_format not in DATAFRAME_FILETYPES:
                return self._get_file(root_path)
            return self._get_dataframe_file(root_path, cols=cols, sep=sep)
        except Exception as e:
            if self._verbose > 1:
                logger.error(f'Cant get file at: {root_path}', exc_info=e)
            return pd.DataFrame()

    def put_file(self, obj, root_path, file_format, date_column=None, mode='upsert', dup_cols=None, lake_dateless_prefix='datalake-raw/raw'):
        '''
        Put a file that gets split by a date column
        '''
        try:
            path, bucket_name, key, file_name, _ = _filter_s3_path(root_path)
            if file_format not in DATAFRAME_FILETYPES:
                res = self._put_file(obj, root_path)
                logger.info(res)
                return
            sep = _get_sep(root_path=root_path, lake_dateless_prefix=lake_dateless_prefix)
            if not self.file_exists(root_path):
                mode = 'refresh'
            df = obj.copy()
            df['lastupdated'] = datetime.datetime.now()

            if date_column is not None:
                df['partition_date'] = pd.to_datetime(df[date_column])

            if mode == 'upsert':
                if date_column is not None:
                    sub_paths = df['partition_date'].dt.year.unique()
                    paths = self.needs_update(root_path=root_path, subpaths=sub_paths)
                    fs_df = pd.DataFrame()
                    if len(paths) != 0:
                        fs_df = pd.concat([self.get_file(root_path=file, cols=df.columns, lake_dateless_prefix=lake_dateless_prefix) for file in paths], ignore_index=True)
                else:
                    fs_df = self.get_file(root_path, cols=df.columns, lake_dateless_prefix=lake_dateless_prefix)

                if fs_df.shape[0] != 0:
                    if dup_cols is None:
                        df = pd.concat([fs_df, df], ignore_index=True).drop_duplicates(keep='last').reset_index().drop(columns='index')
                    else:
                        df = pd.concat([fs_df, df], ignore_index=True).drop_duplicates(dup_cols, keep='last').reset_index().drop(columns='index')

            df = df.loc[:, ~df.columns.duplicated()]  ## Remove duplicated column names ['a','a']
            if date_column is None:
                res = self._put_dataframe_file(df, bucket_name, key, file_name, file_format, sep)
                logger.info(res)
                return
            res = ''
            if 'date' in df.columns:
                df.date = pd.to_datetime(df.date)
                df.date = df.date.dt.strftime('%Y/%m/%d')
            try:
                df['partition_date'] = pd.to_datetime(df['partition_date'])
            except ValueError as e:
                df['partition_date'] = pd.to_datetime(df['partition_date'], utc=True)
            df['partition_date'] = df['partition_date'].dt.strftime('%Y/%m/%d')
            for date, sub_df in df.groupby('partition_date'):
                date = pd.Timestamp(date)
                sub_df.partition_date = pd.to_datetime(sub_df.partition_date)
                sub_df['partition_date'] = sub_df['partition_date'].dt.strftime('%Y/%m/%d')
                res = self._put_dataframe_file(sub_df, bucket_name, key, date.strftime('%Y%m%d'), file_format, sep)
            logger.info(res)
        except Exception as e:
            logger.error(f'Cant put file at: {root_path}', exc_info=e)

    def delete_file(self, root_path):
        try:
            path, bucket_name, key, _, _ = _filter_s3_path(root_path)
            self._client.delete_object(Bucket=bucket_name, Key=key)
            logger.info(f"File deleted: {root_path}")
            return True
        except Exception as e:
            logger.error(f"Error deleting file: {root_path}", exc_info=e)
            return False

    def get_dataset_file(self, root_path, partition_filter=None, cols=None, helper_cols=False):
        try:
            if root_path[-1] != '/':
                root_path = f"{root_path}/"
            if cols is not None:
                if 'lastupdated' not in cols:
                    cols = cols + ['lastupdated']
            df = wr.s3.read_parquet(
                path=root_path,
                dataset=True,
                partition_filter=partition_filter,
                boto3_session=self.session,
                columns=cols,
                use_threads=True,
            )
            if not helper_cols:
                df = df.drop(columns=['partition_year', 'lastupdated'])
            if 'date' in df.columns:
                df.date = pd.to_datetime(df.date)
                df.date = df.date.dt.strftime('%Y/%m/%d')

            return df
        except Exception as e:
            if self._verbose > 1:
                logger.error(f'Cant get file at: {root_path}', exc_info=e)
            return pd.DataFrame()

    def put_dataset_file(self, df, root_path, date_column, partition_cols=None, mode='upsert', dup_cols=None, forced_types=None):
        try:
            if root_path[-1] != '/':
                root_path = f"{root_path}/"
            save_mode = 'overwrite_partitions'
            if not self.file_exists(root_path):
                mode = 'refresh'
                save_mode = 'overwrite'
            df = df.copy()
            df['partition_year'] = pd.to_datetime(df[date_column])
            df['partition_year'] = df['partition_year'].dt.year
            df['partition_year'] = df['partition_year'].astype(str)
            df['lastupdated'] = datetime.datetime.now()
            if mode == 'upsert':
                min_batch_date = df['partition_year'].min()
                max_batch_date = df['partition_year'].max()

                fs_df = self.get_dataset_file(
                    root_path=root_path,
                    partition_filter=lambda x: str(min_batch_date) <= str(x['partition_year']) <= str(max_batch_date),
                    cols=list(df.columns.values),
                    helper_cols=True
                )
                if fs_df.shape[0] != 0:
                    fs_df.partition_year = fs_df['partition_year'].astype(str)
                    if dup_cols is None:
                        df = pd.concat([fs_df, df]).drop_duplicates(keep='last').reset_index().drop(columns='index')
                    else:
                        df = pd.concat([fs_df, df]).drop_duplicates(dup_cols, keep='last').reset_index().drop(columns='index')
                    save_mode = 'overwrite_partitions'

            df = df.loc[:, ~df.columns.duplicated()]  # Remove duplicated column names ['a','a']
            if 'date' in df.columns:
                df.date = pd.to_datetime(df.date)
                df.date = df.date.dt.strftime('%Y/%m/%d')
            res = wr.s3.to_parquet(
                df=df,
                path=root_path,
                dataset=True,
                index=False,
                mode=save_mode,
                partition_cols=partition_cols,
                dtype=forced_types,
                compression='snappy',
                boto3_session=self.session,
                use_threads=True,
            )
            logger.info(res)
        except Exception as e:
            logger.error(f'Put dataset error for {root_path}', exc_info=e)

    def get_last_update_metadata(self, root_path):
        try:
            path, bucket_name, key, file_name, file_format = _filter_s3_path(root_path)
            metadata = None

            for sub_path in list(range(datetime.datetime.now().year, 2000, -1)):
                filter_key = f"{key}/partition_year={sub_path}"
                file = self._client.list_objects_v2(Bucket=bucket_name, Prefix=filter_key, MaxKeys=1)
                if 'Contents' in file:
                    metadata = file['Contents'][0]
                    break

            if 'LastModified' in metadata:
                last_modified = pd.Timestamp(metadata['LastModified'].date() - datetime.timedelta(days=self.refresh_day_shift)).to_pydatetime()
            else:
                last_modified = datetime.date(1900, 1, 1)

            return last_modified
        except Exception as e:
            logger.error(f"Error get_last_update_metadata for {root_path} defaulting to 1900-01-01 ", exc_info=e)
            return datetime.date(1900, 1, 1)

    def _get_file(self, root_path):
        path, bucket_name, key, file_name, file_format = _filter_s3_path(root_path)
        return self._client.get_object(Bucket=bucket_name, Key=f"{key}/{file_name}.{file_format}")

    def _put_file(self, obj, root_path):
        path, bucket_name, key, file_name, file_format = _filter_s3_path(root_path)
        return self._client.put_object(Bucket=bucket_name, Key=f"{key}/{file_name}.{file_format}", Body=obj)

    def _get_dataframe_file(self, root_path, cols, sep):
        try:
            path, bucket_name, key, file_name, file_format = _filter_s3_path(root_path)
            if cols is not None:
                if 'lastupdated' not in cols:
                    cols = cols + ['lastupdated']
            if file_format == 'csv':
                csv_obj = self._client.get_object(Bucket=bucket_name, Key=key)
                if cols is None:
                    df = pd.read_csv(BytesIO(csv_obj['Body'].read()), sep=sep)
                else:
                    df = pd.read_csv(BytesIO(csv_obj['Body'].read()), sep=sep, usecols=cols)
            elif file_format == 'json':
                try:
                    json_obj = self._client.get_object(Bucket=bucket_name, Key=key)
                    df = pd.read_json(StringIO(json_obj['Body'].read()))
                except Exception as e:
                    json_obj = self._client.get_object(Bucket=bucket_name, Key=key)
                    df = json.loads(json_obj['Body'].read().decode('utf-8'))
            else:
                df = pd.DataFrame()
                logger.info('Invalid file_format for get_file no file returned')
            return df
        except Exception as e:
            return pd.DataFrame()

    def _put_dataframe_file(self, df, bucket_name, key, file_name, file_format, sep):
        try:
            if file_format == 'csv':
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False, sep=sep)
                res = self._resource.Object(bucket_name, f"{key}/{file_name}.{file_format}").put(Body=csv_buffer.getvalue())
            elif file_format == 'json':
                csv_buffer = StringIO()
                df.to_json(csv_buffer, index=False)
                res = self._resource.Object(bucket_name, f"{key}/{file_name}.{file_format}").put(Body=csv_buffer.getvalue())
            else:
                res = f'Could not upload file to path: {bucket_name}/{key}'
            return res
        except Exception as e:
            return e


def _filter_s3_path(path):
    try:
        path = path.replace('s3://', '')
        bucket_name, key = path.split('/', 1)
        # Single File was passed
        if len(key.split('.')) > 0:
            file_name, file_format = key.split('.')
            file_name = file_name.split('/')[-1]
        else:  # Group or prefix was passed
            file_name = None
            file_format = None
            if path[-1] != '/':
                path = f"{path}/"
            if key[-1] != '/':
                key = f"{key}/"
        return path, bucket_name, key, file_name, file_format
    except Exception as e:
        raise e


def _get_sep(root_path, lake_dateless_prefix='datalake-raw/raw'):
    """
    Raw dateless sections in the datalake have the seperator of , where the rest of the lake uses |
    :return str:
    """
    return ',' if lake_dateless_prefix in root_path else '|'
