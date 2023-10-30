-- MSSQL
CREATE TABLE [admin].[datastagerpluslog](
	[dataprofilingid] [int] IDENTITY NOT NULL,
	[datastagerplushk] [CHAR](32) NOT NULL,
	[filename] [nvarchar](255) NULL,
	[delimiter] [varchar](5) NULL,
	[targettablename] [varchar](255) NULL,
	[numberofcolumns] [int] NULL,
	[totalrecords] [int] NULL,
	[duplicaterecords] [int] NULL,
	[invalidcharactersrecords] [int] NULL,
	[loadsuccessstatus] [int] NOT NULL,
	[filecreatetime] [datetime] NOT NULL,
	[loadstarttime] [datetime] NOT NULL,
	[loadendtime] [datetime]  NULL
)
GO

CREATE TABLE [admin].[datastagerpluserrorlog](
	[errorlogId] [int] IDENTITY NOT NULL,
	[datastagerplushk] [CHAR](255) NOT NULL,
	[targettablename] [varchar](255) NULL,
	[message] [varchar](MAX) NULL,
	[errordatetime] [datetime] NULL
)
GO

-- POSTGRES
CREATE TABLE admin.datastagerpluslog(
	dataprofilingid int GENERATED ALWAYS AS IDENTITY,
	datastagerplushk CHAR (32) NOT NULL,
	datastagerplusproject CHAR(32) NOT NULL,
	filename varchar(255) NULL,
	targettablename varchar(255) NULL,
	numberofcolumns int NULL,
	totalrecords int NULL,
	duplicaterecords int NULL,
	invalidcharactersrecords int NULL,
	loadsuccessstatus int NOT NULL,
	filecreatetime timestamp NOT NULL,
	loadstarttime timestamp NOT NULL,
	loadendtime timestamp  NULL
);

CREATE TABLE admin.datastagerpluserrorlog(
	errorlogId int GENERATED ALWAYS AS IDENTITY,
	datastagerplushk CHAR(255) NOT NULL,
	targettablename varchar(255) NULL,
	message text NULL,
	errordatetime timestamp NULL
);



