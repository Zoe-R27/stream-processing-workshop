--Which artist has the most out-of-state (from the state the event is in) customers at their events?
SELECT r.eventid, COUNT(*) as num_out_of_state_tickets
FROM
(
select t.customerid, t.eventid
  from ticket t
  join address ac on ac.customerid = t.customerid
  join event e on e.id = t.eventid
  join venue v on e.venueid = v.id
  join address av on av.id = v.addressid
  where av.state != ac.state
 ) as r
GROUP BY r.eventid
ORDER BY num_out_of_state_tickets DESC;

--For the last month, how many events had more than 80% attendance rate in each state?
SELECT event_ticketcnt_capacity.state, COUNT() as num_high_attendance_events
FROM
(
SELECT a.state, e.capacity, COUNT() AS high_attendance_events
FROM event e
JOIN ticket t ON e.id = t.eventid
JOIN venue v ON e.venueid = v.id
JOIN address a ON v.addressid = a.id
WHERE
    e.eventdate > fromDateTime('2026-04-01', 'yyyy-MM-dd')
      AND e.eventdate < fromDateTime('2026-11-01', 'yyyy-MM-dd')
GROUP BY e.id, e.capacity, a.state,  e.eventdate
HAVING
    COUNT(t.id) > 0.80 * e.capacity
) as event_ticketcnt_capacity
GROUP BY event_ticketcnt_capacity.state;
