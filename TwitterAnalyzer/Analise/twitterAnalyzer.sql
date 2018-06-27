USE [master]
GO
/****** Object:  Database [twitterAnalyzer]    Script Date: 27/06/2018 20:43:16 ******/
CREATE DATABASE [twitterAnalyzer]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'twitterAnalyzer', FILENAME = N'C:\Databases\Data\twitterAnalyzer.mdf' , SIZE = 1581056KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'twitterAnalyzer_log', FILENAME = N'C:\Databases\Data\twitterAnalyzer_log.ldf' , SIZE = 401408KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
GO
ALTER DATABASE [twitterAnalyzer] SET COMPATIBILITY_LEVEL = 140
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [twitterAnalyzer].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [twitterAnalyzer] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET ARITHABORT OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [twitterAnalyzer] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [twitterAnalyzer] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET  DISABLE_BROKER 
GO
ALTER DATABASE [twitterAnalyzer] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [twitterAnalyzer] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET RECOVERY SIMPLE 
GO
ALTER DATABASE [twitterAnalyzer] SET  MULTI_USER 
GO
ALTER DATABASE [twitterAnalyzer] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [twitterAnalyzer] SET DB_CHAINING OFF 
GO
ALTER DATABASE [twitterAnalyzer] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [twitterAnalyzer] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO
ALTER DATABASE [twitterAnalyzer] SET DELAYED_DURABILITY = DISABLED 
GO
EXEC sys.sp_db_vardecimal_storage_format N'twitterAnalyzer', N'ON'
GO
ALTER DATABASE [twitterAnalyzer] SET QUERY_STORE = OFF
GO
USE [twitterAnalyzer]
GO
ALTER DATABASE SCOPED CONFIGURATION SET IDENTITY_CACHE = ON;
GO
ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET LEGACY_CARDINALITY_ESTIMATION = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET MAXDOP = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET PARAMETER_SNIFFING = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET QUERY_OPTIMIZER_HOTFIXES = PRIMARY;
GO
USE [twitterAnalyzer]
GO
/****** Object:  UserDefinedFunction [dbo].[fncParticiona5Minutos]    Script Date: 27/06/2018 20:43:16 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[fncParticiona5Minutos]
(
  @dt_twitter datetime
)
RETURNS int
AS
BEGIN
	-- Declare the return variable here
	DECLARE @part int
	set @part = 0

	if (DatePart(MINUTE,@dt_twitter) > 0   and  DatePart(MINUTE,@dt_twitter) <= 10) begin
	  set @part = 10
	end
	else if (DatePart(MINUTE,@dt_twitter) > 10   and  DatePart(MINUTE,@dt_twitter) <= 20) begin
	  set @part = 20
	end
    else if (DatePart(MINUTE,@dt_twitter) > 20   and  DatePart(MINUTE,@dt_twitter) <= 30) begin
	  set @part = 30
	end
    else if (DatePart(MINUTE,@dt_twitter) > 30   and  DatePart(MINUTE,@dt_twitter) <= 40) begin
	  set @part = 40
	end
    else if (DatePart(MINUTE,@dt_twitter) > 40   and  DatePart(MINUTE,@dt_twitter) <= 50) begin
	  set @part = 50
	end
	else begin
	  set @part = 0
	end

	-- Return the result of the function
	RETURN @part

END
GO
/****** Object:  Table [dbo].[entidade]    Script Date: 27/06/2018 20:43:16 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[entidade](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[text] [nvarchar](max) NULL,
	[twitter_id] [bigint] NOT NULL,
 CONSTRAINT [PK__entidade__3213E83F001BF123] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[geo]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[geo](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[type] [varchar](20) NULL,
	[longitude] [decimal](12, 9) NULL,
	[latitude] [decimal](12, 9) NULL,
	[twitter_id] [bigint] NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[hashtag]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[hashtag](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[text] [varchar](8000) NULL,
	[twitter_id] [bigint] NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[place]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[place](
	[_id] [bigint] IDENTITY(1,1) NOT NULL,
	[id] [varchar](100) NOT NULL,
	[url] [varchar](100) NULL,
	[place_type] [varchar](100) NULL,
	[name] [nvarchar](200) NULL,
	[full_name] [nvarchar](200) NULL,
	[country_code] [varchar](5) NULL,
	[country] [varchar](100) NULL,
	[bounding_box] [varchar](8000) NULL,
 CONSTRAINT [PK__tmp_ms_x__DED88B1D10B91855] PRIMARY KEY NONCLUSTERED 
(
	[_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IDX_Place_ID]    Script Date: 27/06/2018 20:43:17 ******/
CREATE UNIQUE CLUSTERED INDEX [IDX_Place_ID] ON [dbo].[place]
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[retwitted_process]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[retwitted_process](
	[twitter_id] [bigint] NOT NULL,
	[level] [int] NOT NULL,
	[twitter_id_original] [bigint] NULL,
	[processed] [datetime] NOT NULL,
	[api_find_processed] [bit] NULL,
PRIMARY KEY CLUSTERED 
(
	[twitter_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[stats_termos]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[stats_termos](
	[id_stats] [bigint] IDENTITY(1,1) NOT NULL,
	[ds_termo] [varchar](100) NOT NULL,
	[dt_referencia] [date] NOT NULL,
	[dt_processamento] [datetime] NOT NULL,
	[qt_positivo] [int] NOT NULL,
	[qt_negativo] [int] NOT NULL,
	[qt_neutro] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[stats_termos_selecao_brasileira]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[stats_termos_selecao_brasileira](
	[id_stats] [bigint] IDENTITY(1,1) NOT NULL,
	[ds_termo] [varchar](100) NOT NULL,
	[dt_referencia] [datetime] NOT NULL,
	[dt_processamento] [datetime] NOT NULL,
	[qt_positivo] [int] NOT NULL,
	[qt_negativo] [int] NOT NULL,
	[qt_neutro] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[stats_users]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[stats_users](
	[id] [bigint] NOT NULL,
	[name] [nvarchar](400) NULL,
	[verified] [bit] NULL,
	[protected] [bit] NULL,
	[Months_Age] [int] NULL,
	[lang] [varchar](10) NULL,
	[statuses_count] [int] NULL,
	[statuses_avg] [int] NULL,
	[default_profile] [bit] NULL,
	[friends_count] [int] NULL,
	[followers_count] [int] NULL,
	[location] [nvarchar](400) NULL,
	[twittes_count] [int] NULL,
	[positive_count] [int] NULL,
	[negative_count] [int] NULL,
	[neutral_count] [int] NULL,
	[statuses_avg_faixa] [int] NULL,
 CONSTRAINT [PK_stats_users] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[sumario_stats_twitters]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[sumario_stats_twitters](
	[PostID] [bigint] NOT NULL,
	[created_at] [datetime] NULL,
	[sentimento] [varchar](20) NULL,
	[Post_Text] [nvarchar](max) NULL,
	[UserID] [bigint] NOT NULL,
	[screen_name] [nvarchar](400) NULL,
	[TotalRespostas] [int] NULL,
	[TotalCitacoes] [int] NULL,
	[TotalRetweeted] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TermoInteresse]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TermoInteresse](
	[id] [uniqueidentifier] NOT NULL,
	[text] [varchar](8000) NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TermoInteresseSentimentoProcessed]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TermoInteresseSentimentoProcessed](
	[id_termo] [uniqueidentifier] NOT NULL,
	[id_post] [bigint] NOT NULL,
	[dt_processed] [datetime] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[teste]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[teste](
	[valor] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[twitter]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[twitter](
	[id] [bigint] NOT NULL,
	[created_at] [datetime] NULL,
	[text] [nvarchar](max) NULL,
	[source] [varchar](8000) NULL,
	[truncated] [bit] NULL,
	[in_reply_to_status_id] [bigint] NULL,
	[in_reply_to_user_id] [bigint] NULL,
	[in_reply_to_screen_name] [nvarchar](100) NULL,
	[user_id] [bigint] NULL,
	[place_id] [varchar](100) NULL,
	[quoted_status_id] [bigint] NULL,
	[is_quote_status] [bit] NULL,
	[retweeted_status_id] [bigint] NULL,
	[quote_count] [int] NULL,
	[reply_count] [int] NULL,
	[retweet_count] [int] NULL,
	[favorite_count] [int] NULL,
	[favorited] [bit] NULL,
	[retweeted] [bit] NULL,
	[possibly_sensitive] [bit] NULL,
	[filter_level] [varchar](20) NULL,
	[lang] [varchar](20) NULL,
	[negativo] [decimal](20, 18) NULL,
	[positivo] [decimal](20, 18) NULL,
	[sentimento] [varchar](20) NULL,
	[crawlered] [datetime] NULL,
	[tags] [varchar](8000) NULL,
 CONSTRAINT [PK__tmp_ms_x__DED88B1DC177F808] PRIMARY KEY NONCLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[user]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[user](
	[id] [bigint] NOT NULL,
	[name] [nvarchar](400) NULL,
	[screen_name] [nvarchar](400) NULL,
	[location] [nvarchar](400) NULL,
	[url] [varchar](200) NULL,
	[description] [nvarchar](4000) NULL,
	[protected] [bit] NULL,
	[verified] [bit] NULL,
	[followers_count] [int] NULL,
	[friends_count] [int] NULL,
	[listed_count] [int] NULL,
	[favourites_count] [int] NULL,
	[statuses_count] [int] NULL,
	[created_at] [datetime] NULL,
	[geo_enabled] [bit] NULL,
	[time_zone] [varchar](200) NULL,
	[lang] [varchar](10) NULL,
	[contributors_enabled] [bit] NULL,
	[default_profile] [bit] NULL,
	[withheld_in_countries] [varchar](100) NULL,
	[withheld_scope] [varchar](100) NULL,
 CONSTRAINT [PK__user__3213E83F322C3F14] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Index [IX_stats_users]    Script Date: 27/06/2018 20:43:17 ******/
CREATE NONCLUSTERED INDEX [IX_stats_users] ON [dbo].[stats_users]
(
	[Months_Age] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_twitter]    Script Date: 27/06/2018 20:43:17 ******/
CREATE NONCLUSTERED INDEX [IX_twitter] ON [dbo].[twitter]
(
	[sentimento] ASC,
	[created_at] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [IX_twitter_1]    Script Date: 27/06/2018 20:43:17 ******/
CREATE NONCLUSTERED INDEX [IX_twitter_1] ON [dbo].[twitter]
(
	[retweeted_status_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [ID_Location]    Script Date: 27/06/2018 20:43:17 ******/
CREATE NONCLUSTERED INDEX [ID_Location] ON [dbo].[user]
(
	[location] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [ID_Stats]    Script Date: 27/06/2018 20:43:17 ******/
CREATE NONCLUSTERED INDEX [ID_Stats] ON [dbo].[user]
(
	[created_at] ASC,
	[statuses_count] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [dbo].[retwitted_process] ADD  DEFAULT ((0)) FOR [level]
GO
ALTER TABLE [dbo].[retwitted_process] ADD  DEFAULT (getdate()) FOR [processed]
GO
ALTER TABLE [dbo].[retwitted_process] ADD  DEFAULT ('FALSE') FOR [api_find_processed]
GO
ALTER TABLE [dbo].[stats_termos] ADD  DEFAULT (getdate()) FOR [dt_processamento]
GO
ALTER TABLE [dbo].[stats_termos] ADD  DEFAULT ((0)) FOR [qt_positivo]
GO
ALTER TABLE [dbo].[stats_termos] ADD  DEFAULT ((0)) FOR [qt_negativo]
GO
ALTER TABLE [dbo].[stats_termos] ADD  DEFAULT ((0)) FOR [qt_neutro]
GO
ALTER TABLE [dbo].[stats_termos_selecao_brasileira] ADD  DEFAULT (getdate()) FOR [dt_processamento]
GO
ALTER TABLE [dbo].[stats_termos_selecao_brasileira] ADD  DEFAULT ((0)) FOR [qt_positivo]
GO
ALTER TABLE [dbo].[stats_termos_selecao_brasileira] ADD  DEFAULT ((0)) FOR [qt_negativo]
GO
ALTER TABLE [dbo].[stats_termos_selecao_brasileira] ADD  DEFAULT ((0)) FOR [qt_neutro]
GO
ALTER TABLE [dbo].[TermoInteresseSentimentoProcessed] ADD  DEFAULT (getdate()) FOR [dt_processed]
GO
ALTER TABLE [dbo].[twitter] ADD  CONSTRAINT [DF_twitter_crawlered]  DEFAULT (getdate()) FOR [crawlered]
GO
ALTER TABLE [dbo].[retwitted_process]  WITH CHECK ADD  CONSTRAINT [fk_retwitted_process] FOREIGN KEY([twitter_id_original])
REFERENCES [dbo].[retwitted_process] ([twitter_id])
GO
ALTER TABLE [dbo].[retwitted_process] CHECK CONSTRAINT [fk_retwitted_process]
GO
/****** Object:  StoredProcedure [dbo].[prcProcessaTermosInteresesSentimento]    Script Date: 27/06/2018 20:43:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create procedure [dbo].[prcProcessaTermosInteresesSentimento]
as

declare @id_termo uniqueidentifier
--set @id_termo ='115F28DD-4AE9-40FA-9F68-1D25B3A092ED'
declare @search varchar(8000)
--set @search = (select [text] from termointeresse where [id] = @id_termo)


declare cr_termos cursor 
  for select [id],[text] from termointeresse

open cr_termos
FETCH NEXT FROM cr_termos  INTO @id_termo, @search 

WHILE @@FETCH_STATUS = 0  
BEGIN

	if (@search is null)  begin
	  raiserror( 'Termo de interesse vazio ou não encontrado',16,1)
	end

	delete from AnaliseSentimento where id_termo = @id_termo

	insert into AnaliseSentimento
	select min(@id_termo) as id_termo,dt_referencia,
		   sum(case when sentimento = 'POSITIVO' then 1 else 0 end) qt_positivo,
		   sum(case when sentimento = 'NEUTRO' then 1 else 0 end) qt_neutro,
		   sum(case when sentimento = 'NEGATIVO' then 1 else 0 end) qt_negativo
	from(
		select @search as termo,
			   cast(a.created_at as date) as dt_referencia,
			   sentimento
		  from twitter a
		where a.text like '%'+ @search + '%'
		  and sentimento = 'POSITIVO'
		union all
		select @search as termo,
			   cast(a.created_at as date) as dt_referencia,
			   sentimento
		  from twitter a
		where a.text like '%'+ @search + '%'
		  and sentimento = 'NEGATIVO'
		union all
		select @search as termo,
			   cast(a.created_at as date) as dt_referencia,
			   sentimento
		  from twitter a
		where a.text like '%'+ @search + '%'
		  and sentimento = 'NEUTRO'
	) as T
	group by dt_referencia
	FETCH NEXT FROM cr_termos  INTO @id_termo, @search 
end
GO
USE [master]
GO
ALTER DATABASE [twitterAnalyzer] SET  READ_WRITE 
GO
