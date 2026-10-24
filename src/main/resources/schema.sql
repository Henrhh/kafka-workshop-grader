create table if not exists team (
    name        varchar(32) primary key,
    created_at  timestamp with time zone not null
);

create table if not exists attempt (
    id           varchar(64) primary key,
    team         varchar(32) not null references team(name),
    level        int not null,
    status       varchar(16) not null,
    message      text,
    started_at   timestamp with time zone not null,
    finished_at  timestamp with time zone
);

create index if not exists attempt_team_level on attempt(team, level);

create table if not exists solve (
    team             varchar(32) not null references team(name),
    level            int not null,
    points           int not null,
    solve_rank       int not null,
    failed_attempts  int not null,
    bonus            int not null,
    solved_at        timestamp with time zone not null,
    primary key (team, level)
);
