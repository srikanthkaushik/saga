alter table reservation add column status varchar(20) not null default 'RESERVED';
alter table reservation add column released_at timestamptz;
