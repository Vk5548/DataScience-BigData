select distinct(pid)
from knownfor where pid in

(select distinct(pid) from knownfor join person as p0 join movie as mov
on p0.id = pid and mov.id = mid
where mid in
(select d.mid
from  director as d join person as p2 join movie as m2
on d.pid = p2.id and d.mid = m2.id
where p2.name = 'Sofia Coppola'))

and pid in
(select distinct(pid) from knownfor join person as p0 join movie as mov
on p0.id = pid and mov.id = mid
where mid in (select a.mid
from  actor as a join person as p join movie as m
on  a.pid = p.id and a.mid = m.id
where p.name = 'Antonio Banderas'))