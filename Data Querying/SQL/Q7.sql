select distinct(w.pid)
from writer as w join person as p join movie as m
on w.pid = p.id and w.mid = m.id
where match(m.otitle) against('Jesus') and match(m.otitle) against('Christ')
and m.rating >
 ALl(select rating
from director as d join person as p2 join movie as m2
on d.pid = p2.id and d.mid = m2.id
where p2.name ='Edward D. Wood Jr.')