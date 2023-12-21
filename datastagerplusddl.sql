-- MSSQL
CREATE TABLE [_admin].[datafilestagelog](
	[dataprofilingid] [int] IDENTITY(1,1) NOT NULL,
	[datafilestagehk] [char](32) NOT NULL,
	[filename] [nvarchar](255) NOT NULL,
	[delimiter] [varchar](5) NULL,
	[targettablename] [varchar](255) NOT NULL,
	[schemaname] [varchar](100) NOT NULL,
	[numberofcolumns] [int] NULL,
	[totalrecords] [int] NULL,
	[duplicaterecords] [int] NULL,
	[invalidcharactersrecords] [int] NULL,
	[loadsuccessstatus] [int] NOT NULL,
	[filecreatetime] [datetime] NOT NULL,
	[loadstarttime] [datetime] NOT NULL,
	[loadtomemoryendtime] [datetime] NULL,
	[loadendtime] [datetime] NULL
) 
GO

CREATE TABLE [_admin].[datafilestageerrorlog](
	[errorlogId] [int] IDENTITY(1,1) NOT NULL,
	[datafilestagehk] [char](255) NOT NULL,
	[targettablename] [varchar](255) NULL,
	[message] [varchar](max) NULL,
	[errordatetime] [datetime] NULL
)
GO

-- POSTGRES
CREATE TABLE _admin.datafilestagelog(
	dataprofilingid int GENERATED ALWAYS AS IDENTITY,
	datafilestagehk CHAR (32) NOT NULL,
	filename varchar(255) NULL,
	delimiter varchar(5) NULL,
	targettablename varchar(255) NULL,
	schemaname varchar(100) NOT NULL,
	numberofcolumns int NULL,
	totalrecords int NULL,
	duplicaterecords int NULL,
	invalidcharactersrecords int NULL,
	loadsuccessstatus int NOT NULL,
	filecreatetime timestamp NOT NULL,
	loadstarttime timestamp NOT NULL,
	loadtomemoryendtime timestamp NULL,
	loadendtime timestamp  NULL
);

CREATE TABLE _admin.datafilestageerrorlog(
	errorlogId int GENERATED ALWAYS AS IDENTITY,
	datafilestagehk CHAR(255) NOT NULL,
	targettablename varchar(255) NULL,
	message text NULL,
	errordatetime timestamp NULL
);




