create table if not exists students (
  stlastname text not null,
  stfirstname text not null,
  grade int8 not null,
  classroom int8 not null,
  bus int8 not null,
  gpa double precision not null,
  tlastname text not null,
  tfirstname text not null
);

create index if not exists "students_bus_idx" on students using btree (bus);
create index if not exists "students_grade_idx" on students using btree (grade);
create index if not exists "students_stlastname_idx" on students using btree (stlastname);
create index if not exists "students_tfirstname_tlastname_idx" on students using btree (tfirstname, tlastname);

-- \copy students from 'students.txt' with (format csv);
