with recursive search(node, depth, is_solved, empty_idx, prev_empty_idx, next_empty_idx, history, depth_solved, goal) as ( 
	select
		root.node
	,	0 as depth
	,	case when root.node = root.goal then 1 else 0 end is_solved
	,	case 
			when root.node[1] = 0 then 1
			when root.node[2] = 0 then 2
			when root.node[3] = 0 then 3
			when root.node[4] = 0 then 4
			when root.node[5] = 0 then 5
			when root.node[6] = 0 then 6
			when root.node[7] = 0 then 7
			when root.node[8] = 0 then 8
			when root.node[9] = 0 then 9
		end as empty_idx
	,	-1 as prev_empty_idx
	,	unnest(case 
			when root.node[1] = 0 then array[2, 4]
			when root.node[2] = 0 then array[1, 3, 5]
			when root.node[3] = 0 then array[2, 6]
			when root.node[4] = 0 then array[1, 5, 7]
			when root.node[5] = 0 then array[2, 4, 6, 8]
			when root.node[6] = 0 then array[3, 5, 9]
			when root.node[7] = 0 then array[4, 8]
			when root.node[8] = 0 then array[5, 7, 9]
			when root.node[9] = 0 then array[6, 8]
		end) as next_empty_idx
	,	root.node::text as history
	,	case when root.node = root.goal then 1 else 0 end depth_solved
	,	root.goal
	from
		(select
			/* 
			settings for root node and goal 
  			 - top left to bottom right 
  			 - 0 is blank
  			*/
			array[8,3,5,4,1,6,2,7,0] as node
		,	array[1,2,3,8,0,4,7,6,5] as goal
		)root
	union all
	select
		vv.node
	,	vv.depth
	,	vv.is_solved
	,	vv.empty_idx
	,	vv.prev_empty_idx
	,	vv.next_empty_idx
	,	vv.history
	,	max(vv.is_solved)over(partition by depth) as depth_solved
	,	vv.goal
	from
		(select
			v.node
		,	v.depth
		,	case when v.node = v.goal then 1 else 0 end is_solved
		,	v.next_empty_idx as empty_idx
		,	v.prev_empty_idx
		,	unnest(case 
					when v.node[1] = 0 then array[2, 4]
					when v.node[2] = 0 then array[1, 3, 5]
					when v.node[3] = 0 then array[2, 6]
					when v.node[4] = 0 then array[1, 5, 7]
					when v.node[5] = 0 then array[2, 4, 6, 8]
					when v.node[6] = 0 then array[3, 5, 9]
					when v.node[7] = 0 then array[4, 8]
					when v.node[8] = 0 then array[5, 7, 9]
					when v.node[9] = 0 then array[6, 8]
			end) as next_empty_idx
		,	history || ' | ' || v.node::text as history
		,	v.goal
		from
			(select
				array[
					node[case when 1 in (empty_idx, next_empty_idx) then empty_idx + next_empty_idx - 1 else 1 end]
				,	node[case when 2 in (empty_idx, next_empty_idx) then empty_idx + next_empty_idx - 2 else 2 end]
				,	node[case when 3 in (empty_idx, next_empty_idx) then empty_idx + next_empty_idx - 3 else 3 end]
				,	node[case when 4 in (empty_idx, next_empty_idx) then empty_idx + next_empty_idx - 4 else 4 end]
				,	node[case when 5 in (empty_idx, next_empty_idx) then empty_idx + next_empty_idx - 5 else 5 end]
				,	node[case when 6 in (empty_idx, next_empty_idx) then empty_idx + next_empty_idx - 6 else 6 end]
				,	node[case when 7 in (empty_idx, next_empty_idx) then empty_idx + next_empty_idx - 7 else 7 end]
				,	node[case when 8 in (empty_idx, next_empty_idx) then empty_idx + next_empty_idx - 8 else 8 end]
				,	node[case when 9 in (empty_idx, next_empty_idx) then empty_idx + next_empty_idx - 9 else 9 end]
				]	as node
				,	depth + 1 as depth
				,	is_solved as is_solved
				,	next_empty_idx as next_empty_idx
				,	empty_idx as prev_empty_idx
				,	history as history
				,	goal as goal
			from
				search
			where
				depth_solved = 0 
			)v
		)vv
	where
		vv.prev_empty_idx <> vv.next_empty_idx
)
select distinct history, depth from search where is_solved = 1
