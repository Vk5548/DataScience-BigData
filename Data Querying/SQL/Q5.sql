Select a.pid,  COUNT(m.id)
from Actor as a Join Movie as m join Person as p join moviegenre as mg join genre as g
on p.id = a.pid and m.id = a.mid and m.id = mg.mid and g.id = mg.gid
 
where  
 dyear is NULL and g.name = 'Drama' and 
 (p.name like '% Raj'
or
 p.name like '% Patel')
and a.pid not in 
	(select a2.pid
	from Actor as a2 Join Movie as m2 join Person as p2 join moviegenre as mg2 join genre as g2
    on p2.id = a2.pid and m2.id = a2.mid and m2.id = mg2.mid and g2.id = mg2.gid
  where g2.name = 'Comedy'
     Group by a2.pid
having COUNT(m2.id) <> 0)
Group by a.pid
having COUNT(m.id) >= 5;