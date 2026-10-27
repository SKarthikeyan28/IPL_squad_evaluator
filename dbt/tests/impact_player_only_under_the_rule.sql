-- Impact Player substitutions appear only in seasons where rule_changes.csv says the rule
-- applied: the sourced reference data and the match data must agree.
select r.match_id, r.season
from {{ ref('stg_silver__replacements') }} r
join {{ ref('stg_silver__matches') }} m using (match_id)
join {{ ref('dim_season') }} s on s.competition = m.competition and s.season = m.season
where r.reason = 'impact_player' and not s.impact_player_rule
