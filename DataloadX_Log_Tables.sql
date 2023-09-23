CREATE TABLE [admin].[DATALOADX_LOG](
	[DataProfilingID] [int] IDENTITY NOT NULL,
	[DataloadX_HK] [CHAR](32) NOT NULL,
	[DataloadX_Project] [CHAR](32) NOT NULL,
	[Filename] [nvarchar](255) NULL,
	[TargetTableName] [varchar](255) NULL,
	[NumberOfColumns] [int] NULL,
	[TotalRecords] [int] NULL,
	[DuplicateRecords] [int] NULL,
	[InvalidCharactersRecords] [int] NULL,
	[LoadSuccessStatus] [int] NOT NULL,
	[FileDropTime] [datetime] NOT NULL,
	[ProfileCreateTime] [datetime] NOT NULL,
	[LoadCompleteTime] [datetime]  NULL,
PRIMARY KEY CLUSTERED
(
	[DataProfilingID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [admin].[DATALOADX_ERROR_LOG](
	[ErrorLogId] [int] IDENTITY NOT NULL,
	[DataloadX_HK] [CHAR](32) NOT NULL,
	[TargetTableName] [varchar](255) NULL,
	[Message] [varchar](MAX) NULL,
	[ErrorDateTime] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[ErrorLogId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO



