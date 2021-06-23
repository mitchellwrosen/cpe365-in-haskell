{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Cpe365 where

import Control.Exception
import Control.Monad
import Control.Monad.Logger (NoLoggingT, runNoLoggingT)
import Data.ByteString (ByteString)
import Data.Coerce (coerce)
import Data.Functor.Apply
import Data.Int
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Text.IO as Text
import qualified Database.PostgreSQL.Query as Pq
import qualified Database.PostgreSQL.Simple as Ps
import qualified Database.PostgreSQL.Simple.Types as Ps
import GHC.Generics (Generic)
import qualified Hasql.Connection as Hasql
import qualified Rel8

------------------------------------------------------------------------------------------------------------------------
-- Query 1
--
-- Given a student’s last name, find the student’s grade, classroom and teacher (if there is more than one student with
-- the same last name, find this information for all students)

pq_1 :: Text -> Pq [(Double, Int64, Text, Text)]
pq_1 name =
  Pq.pgQuery
    [Pq.sqlExp|
      select gpa, classroom, tfirstname, tlastname
      from students
      where stlastname = #{name}
    |]

rel8_1 :: Text -> Rel8.Query (Rel8.Expr Double, Rel8.Expr Int64, Rel8.Expr Text, Rel8.Expr Text)
rel8_1 name = do
  student <- Rel8.each studentsSchema
  Rel8.where_ (stlastname student Rel8.==. Rel8.lit name)
  pure (gpa student, classroom student, tfirstname student, tlastname student)

------------------------------------------------------------------------------------------------------------------------
-- Query 2
--
-- Given a student’s last name, find the bus route the student takes (if there is more than one student with the same
-- last name, find this information for all students)

pq_2 :: Text -> Pq [Pq.Only Int64]
pq_2 name =
  Pq.pgQuery
    [Pq.sqlExp|
      select bus
      from students
      where stlastname = #{name}
    |]

rel8_2 :: Text -> Rel8.Query (Rel8.Expr Int64)
rel8_2 name = do
  student <- Rel8.each studentsSchema
  Rel8.where_ (stlastname student Rel8.==. Rel8.lit name)
  pure (bus student)

------------------------------------------------------------------------------------------------------------------------
-- Query 3
--
-- Given a teacher, find the list of students in his/her class

pq_3 :: Text -> Text -> Pq [(Text, Text)]
pq_3 fname lname =
  Pq.pgQuery
    [Pq.sqlExp|
      select stfirstname, stlastname
      from students
      where tfirstname = #{fname}
        and tlastname = #{lname}
    |]

rel8_3 :: Text -> Text -> Rel8.Query (Rel8.Expr Text, Rel8.Expr Text)
rel8_3 fname lname = do
  student <- Rel8.each studentsSchema
  Rel8.where_ (tfirstname student Rel8.==. Rel8.lit fname)
  Rel8.where_ (tlastname student Rel8.==. Rel8.lit lname)
  pure (stfirstname student, stlastname student)

------------------------------------------------------------------------------------------------------------------------
-- Query 4
--
-- Given a bus route, find all students who take it

pq_4 :: Int64 -> Pq [(Text, Text)]
pq_4 n =
  Pq.pgQuery
    [Pq.sqlExp|
      select stfirstname, stlastname
      from students
      where bus = #{n}
    |]

rel8_4 :: Int64 -> Rel8.Query (Rel8.Expr Text, Rel8.Expr Text)
rel8_4 n = do
  student <- Rel8.each studentsSchema
  Rel8.where_ (bus student Rel8.==. Rel8.lit n)
  pure (stfirstname student, stlastname student)

------------------------------------------------------------------------------------------------------------------------
-- Query 5
--
-- Find all students at a specified grade level

pq_5 :: Int64 -> Pq [(Text, Text)]
pq_5 n =
  Pq.pgQuery
    [Pq.sqlExp|
      select stfirstname, stlastname
      from students
      where grade = #{n}
    |]

rel8_5 :: Int64 -> Rel8.Query (Rel8.Expr Text, Rel8.Expr Text)
rel8_5 n = do
  student <- Rel8.each studentsSchema
  Rel8.where_ (grade student Rel8.==. Rel8.lit n)
  pure (stfirstname student, stlastname student)

------------------------------------------------------------------------------------------------------------------------

explain :: ByteString -> Text -> IO ()
explain connstr query =
  ps_with connstr \conn -> do
    explanation <- Ps.query_ conn (Ps.Query (Text.encodeUtf8 ("explain " <> query)))
    Text.putStrLn (Text.unlines (coerce @[Ps.Only Text] explanation))

------------------------------------------------------------------------------------------------------------------------
-- hasql definitions

hasql_with :: ByteString -> (Hasql.Connection -> IO a) -> IO a
hasql_with connstr action =
  bracket (Hasql.acquire connstr) (either (const (pure ())) Hasql.release) (either undefined action)

------------------------------------------------------------------------------------------------------------------------
-- postgresql-query definitions

type Pq a =
  Pq.PgMonadT (NoLoggingT IO) a

pq_run :: ByteString -> Pq a -> IO a
pq_run connstr action =
  ps_with connstr \conn ->
    runNoLoggingT (Pq.runPgMonadT conn action)

------------------------------------------------------------------------------------------------------------------------
-- postgresql-simple definitions

ps_with :: ByteString -> (Ps.Connection -> IO a) -> IO a
ps_with connstr =
  bracket (Ps.connectPostgreSQL connstr) Ps.close

------------------------------------------------------------------------------------------------------------------------
-- rel8 definitions

rel8_explain :: Rel8.Table Rel8.Expr a => ByteString -> Rel8.Query a -> IO ()
rel8_explain connstr query =
  explain connstr (Text.pack (Rel8.showQuery query))

rel8_print :: Rel8.Table Rel8.Expr a => Rel8.Query a -> IO ()
rel8_print =
  putStrLn . Rel8.showQuery

rel8_run :: Rel8.Serializable a b => ByteString -> Rel8.Query a -> IO [b]
rel8_run connstr query =
  hasql_with connstr \conn -> Rel8.select conn query

data StudentsTable f = StudentsTable
  { stlastname :: Rel8.Column f Text,
    stfirstname :: Rel8.Column f Text,
    grade :: Rel8.Column f Int64,
    classroom :: Rel8.Column f Int64,
    bus :: Rel8.Column f Int64,
    gpa :: Rel8.Column f Double,
    tlastname :: Rel8.Column f Text,
    tfirstname :: Rel8.Column f Text
  }
  deriving stock (Generic)
  deriving anyclass (Rel8.Rel8able)

deriving stock instance (f ~ Rel8.Result) => Show (StudentsTable f)

studentsSchema :: Rel8.TableSchema (StudentsTable Rel8.Name)
studentsSchema =
  Rel8.TableSchema
    { Rel8.name = "students",
      Rel8.schema = Nothing,
      Rel8.columns =
        StudentsTable
          { stlastname = "stlastname",
            stfirstname = "stfirstname",
            grade = "grade",
            classroom = "classroom",
            bus = "bus",
            gpa = "gpa",
            tlastname = "tlastname",
            tfirstname = "tfirstname"
          }
    }
