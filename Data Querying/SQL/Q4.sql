Select p.id, count(m.id), truncate(avg(round(rating, 2)), 2)
from person as p join director as d join movie as m join genre as g
 join moviegenre as mg on m.id = d.mid and p.id = d.pid and m.id = mg.mid and g.id = mg.gid
where g.name = "Sci-Fi" and m.totalvotes >= 1000
group by p.id
having count(m.id) >= 5
order by avg(round(rating, 2)) asc
limit 15
