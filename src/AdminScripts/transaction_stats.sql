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
			,cs.startqueue as xid_startqueue
			,cs.startwork
			,cs.endtime commit_endtime
			,cs.queuelen
			,xb.newblocks
			,xb.dirtyblocks
			,xb.headers
			,datediff(ms,s.starttime,s.endtime) cmd_dur_ms
			,datediff(ms,cs.startqueue,cs.startwork) commit_queue_dur_ms
			,datediff(ms,cs.startqueue,cs.endtime) total_commit_dur_ms
			,datediff(ms,cs.startwork,cs.endtime) commit_work_dur_ms
			,rank () over (partition by xid order by s.starttime) as rank
		FROM svl_statementtext s
		JOIN stl_commit_stats cs USING (xid)
		JOIN xid_blocks xb USING (xid)
		WHERE (1=1)
			AND s.sequence = 0
			AND s.userid > 1
			AND cs.node = -1
			AND cs.xid > 0
			AND cs.endtime > cs.startqueue),
	cmds AS
		(SELECT 
			xid
			,LISTAGG(cmd,'|') WITHIN GROUP (ORDER BY xids.rank) cmd_pattern
			,SUM(cmd_dur_ms) xid_cmds_dur_sum_ms
			,datediff(ms,MIN(cmd_starttime),MAX(xid_startqueue)) xid_cmds_dur_ms
			,datediff(ms,MIN(cmd_starttime),MAX(commit_endtime)) total_xid_dur_ms 
			,AVG(xids.newblocks) xid_newblocks
			,AVG(xids.dirtyblocks) xid_dirtyblocks
			,AVG(xids.headers) xid_headers
		FROM
			xids
		WHERE (1=1)
			AND cmd <> 'padb_fetch_sample'
		GROUP BY xid)
SELECT cmd_pattern
	,COUNT(*)
	,MIN(xid_newblocks) min_xid_newblocks
	,AVG(xid_newblocks) avg_xid_newblocks
	,MAX(xid_newblocks) max_xid_newblocks
	,MIN(xid_dirtyblocks) min_xid_dirtyblocks
	,AVG(xid_dirtyblocks) avg_xid_dirtyblocks
	,MAX(xid_dirtyblocks) max_xid_dirtyblocks
	,MIN(xid_headers) min_xid_headers
	,AVG(xid_headers) avg_xid_headers
	,MAX(xid_headers) max_xid_headers
	,AVG(xid_cmds_dur_sum_ms) avg_xid_cmds_dur_sum_ms
	,AVG(xid_cmds_dur_ms) avg_xid_cmds_dur_ms
	,AVG(total_xid_dur_ms) avg_total_xid_dur_ms
FROM
	cmds
GROUP BY 
	cmd_pattern
ORDER BY 2 DESC;
