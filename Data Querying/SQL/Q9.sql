select k.pid, count(k.mid),  truncate(round(avg(rating), 2), 2)
from person as p join knownfor as k join movie as m join moviegenre as mg join genre as g
on p.id = k.pid and m.id = k.mid and mg.mid = m.id and g.id = mg.gid
where p.dyear is NULL and p.name like 'Steve%'
and k.mid in (
select m2.id
from movie as m2 join director as d join person as p2
on p2.id = d.pid and d.mid = m2.id 
group by m2.id 
having count(d.pid) = 1)
and  (g.name = 'Drama' or g.name = 'Thriller' and 
(select count(mg2.mid) from moviegenre as mg2 join genre as g2
on mg2.gid = g2.id
where mg2.mid = k.mid and (g2.name = 'Drama' or g2.name = 'Thriller')) <=1)
group by k.pid
having count(k.mid) >= 4 
order by truncate(round(avg(m.rating), 2), 2) desc
