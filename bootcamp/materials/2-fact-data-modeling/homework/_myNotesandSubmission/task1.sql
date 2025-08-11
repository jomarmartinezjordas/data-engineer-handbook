-- task 1: deduplicate game_details
create temporary table temp_game_details as
with deduplicated_game_details as (
    select
        *,
        row_number() over (partition by game_id, team_id, player_id) as row_num
    from game_details gd
)

select *
from deduplicated_game_details
where row_num = 1;

-- insert the deduplicated data into the game_details table
truncate table game_details;

insert into game_details
select game_id, team_id, team_abbreviation, team_city, player_id, player_name, nickname,
start_position, comment, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, 
reb, ast, stl, blk, "to", pf, pts, plus_minus 
from temp_game_details;