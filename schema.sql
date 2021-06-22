create table if not exists students (
  stlastname text,
  stfirstname text,
  grade int8,
  classroom int8,
  bus int8,
  gpa double precision,
  tlastname text,
  tfirstname text
);
-- \copy students from 'students.txt' with (format csv);
