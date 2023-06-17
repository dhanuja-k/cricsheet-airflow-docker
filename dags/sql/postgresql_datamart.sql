-- Create Combined SQL View

CREATE OR REPLACE VIEW t20_matches_view AS
SELECT 
f.match_id, d.balls_per_over, d.bowl_out, d.city, d.date, f.start_date, d.eliminator, d.event, d.gender,
 d.match_number, d.method, d.outcome, d.player_of_match, d.season,
  d.toss_decision, d.toss_uncontested, d.toss_winner, 
  d.venue, d.winner, d.winner_runs, d.winner_wickets, 
  f.innings, f.ball, f.batting_team, f.bowling_team, 
  f.striker, f.non_striker, f.bowler, f.runs_off_bat, f.extras, f.wides, f.noballs, f.byes, f.legbyes, 
  f.penalty, f.wicket_type, f.player_dismissed, f.other_wicket_type, f.other_player_dismissed  
FROM fact_innings AS f
JOIN dim_matches AS d
ON f.match_id = d.filename;

