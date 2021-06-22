{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}

module Cpe365 where

import Control.Exception
import Data.Functor.Apply
import Data.Int
import Data.Text (Text)
import GHC.Generics (Generic)
import qualified Hasql.Connection as Hasql
import qualified Rel8

------------------------------------------------------------------------------------------------------------------------
-- rel8 definitions

data StudentsTable f = StudentsTable
  { stlastname :: Rel8.Column f (Maybe Text),
    stfirstname :: Rel8.Column f (Maybe Text),
    grade :: Rel8.Column f (Maybe Int64),
    classroom :: Rel8.Column f (Maybe Int64),
    bus :: Rel8.Column f (Maybe Int64),
    gpa :: Rel8.Column f (Maybe Double),
    tlastname :: Rel8.Column f (Maybe Text),
    tfirstname :: Rel8.Column f (Maybe Text)
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

------------------------------------------------------------------------------------------------------------------------
-- Query 1
--
-- Given a student’s last name, find the student’s grade, classroom and teacher (if there is more than one student with
-- the same last name, find this information for all students);
--
-- select stlastname, array_agg(row(gpa,classroom,tfirstname,tlastname))
-- from students
-- group by stlastname;
rel8_1 ::
  Rel8.Query
    ( Rel8.Expr (Maybe Text),
      Rel8.ListTable (Rel8.Expr (Maybe Double), Rel8.Expr (Maybe Int64), Rel8.Expr (Maybe Text), Rel8.Expr (Maybe Text))
    )
rel8_1 = do
  Rel8.aggregate do
    student <- Rel8.each studentsSchema
    pure
      ( Rel8.groupBy (stlastname student),
        Rel8.listAgg (gpa student, classroom student, tfirstname student, tlastname student)
      )
