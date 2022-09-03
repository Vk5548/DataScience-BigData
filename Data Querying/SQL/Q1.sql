Select pid,  COUNT(mid)
from Actor as a Join Movie as m join Person as p on p.id = a.pid and m.id = a.mid
 where m.year = 2016 
and runtime > (90) and dyear is NULL 
 and pid not in 
	(select pid
	from Actor as A Join Movie as M join Person as p on p.id = a.pid and m.id = a.mid
  where year >= 2017
     Group by pid
having COUNT(mid) <> 0)
Group by pid
having COUNT(m.id) > 3;


