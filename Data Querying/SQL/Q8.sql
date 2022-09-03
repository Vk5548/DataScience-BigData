Select p.pid, count(m.id)
from producer as p join movie as m join genre as g join moviegenre as mg
on g.id = mg.gid and m.id = p.mid and mg.mid = m.id
where g.name = "Action"

and p.pid in
(select d.pid
from director as d
group by d.pid
having count(d.mid) >= 1)

and p.pid not in
(select p2.pid
from producer as p2 join movie as m2 join genre as g2 join moviegenre as mg2
on g2.id = mg2.gid and m2.id = p2.mid and mg2.mid = m2.id
where g2.name = "Romance"
group by p2.pid
having count(m2.id) > 0)




group by p.pid
having count(m.id)>=15






