SELECT m.id, m.otitle, m.runtime
FROM movie as m join moviegenre as mg join genre as g on m.id = mg.mid and g.id = mg.gid
where (year between 1980 and 1999 ) and rating > 6.5 and totalvotes > 10000 and g.name = 'Comedy'
and (
Select COUNT(m2.id)
FROM movie as m2 join moviegenre as mg2 join genre as g2 on m2.id = mg2.mid and g2.id = mg2.gid
where m2.year > m.year and m2.year <= 1999 and m2.otitle like concat(m.otitle,'%') and m2.id <> m.id and g2.name ='Comedy')  > 0

