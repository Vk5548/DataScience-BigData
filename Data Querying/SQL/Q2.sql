SELECT p.id, COUNT(mid)
FROM movie as m join actor as a join person as p on p.id = a.pid and m.id = a.mid
where dyear is NULL and adult = 1 and year = 2021
and p.id not in
 (SELECT p.id
 FROM movie as m join actor as a join person as p on p.id = a.pid and m.id = a.mid
 where adult = 1 and year < 2021
 group by p.id
 having count(mid) <> 0
 )
 group by p.id
order by  COUNT(mid) desc, p.id
LIMIT 25;