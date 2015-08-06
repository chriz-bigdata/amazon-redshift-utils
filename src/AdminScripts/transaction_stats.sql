WITH 
	xid_blocks AS
		(SELECT cs.xid
			,SUM(cs.newblocks) newblocks
			,SUM(cs.dirtyblocks) dirtyblocks
			,SUM(cs.headers) headers
		FROM
			stl_commit_stats cs
		WHERE (1=1)
			AND cs.xid > 0
			AND cs.endtime > cs.startqueue
		GROUP BY cs.xid),
	xids AS 
		(SELECT xid
			,split_part(lower(trim(replace(replace(s.text,';',''),':',''))),' ',1) cmd
			,s.starttime cmd_starttime
			,s.endtime cmd_endtime
			,cs.startqueue 
			,cs.startwork
			,cs.endtime commit_endtime
			,cs.queuelen
			,xb.newblocks
			,xb.dirtyblocks
			,xb.headers
			-- Add xid_dur_ms where rank=1 for s.starttime until cs.startqueue
			,datediff(ms,s.starttime,s.endtime) cmd_dur_ms
			,datediff(ms,cs.startqueue,cs.startwork) commit_queue_dur_ms
			,datediff(ms,cs.startqueue,cs.endtime) total_commit_dur_ms
			,datediff(ms,cs.startwork,cs.endtime) commit_work_dur_ms
			,rank () over (partition by xid order by s.starttime) as rank
		FROM svl_statementtext s
		JOIN stl_commit_stats cs USING (xid)
		JOIN xid_blocks xb USING (xid)
		WHERE (1=1)
			AND s.sequence=0
			AND cs.node = -1
			AND cs.xid > 0
			AND cs.endtime > cs.startqueue)
SELECT 
	xid
	,LISTAGG(cmd,'|') WITHIN GROUP (ORDER BY xids.rank)
	,SUM(cmd_dur_ms) xid_cmd_dur_ms
	,AVG(newblocks) xid_newblocks
	,AVG(dirtyblocks) xid_dirtyblocks
	,AVG(headers) xid_headers
FROM
	xids
GROUP BY xid;
