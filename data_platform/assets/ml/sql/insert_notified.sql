insert into elo.notified (global_id)
values (: global_id)
on conflict (global_id) do nothing
