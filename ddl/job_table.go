package ddl

import (
	"context"
	"fmt"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

func (d *ddl) getOneGeneralJob(sess sessionctx.Context) (*model.Job, error) {
	sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "begin")
	defer func() {
		sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "commit")
	}()
	runningOrBlockedIDs := make([]string, 0, 10)
	for id := range d.runningReorgJobMap {
		runningOrBlockedIDs = append(runningOrBlockedIDs, strconv.Itoa(id))
	}
	var sql string
	if len(runningOrBlockedIDs) == 0 {
		sql = "select job_meta from mysql.tidb_ddl_job where job_id = (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id having not reorg order by min(job_id) limit 1)"
	} else {
		sql = fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where job_id = (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id having not reorg and job_id not in (%s) order by min(job_id) limit 1)", strings.Join(runningOrBlockedIDs, ", "))
	}
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return nil, err
	}
	var rows []chunk.Row
	rows, err = sqlexec.DrainRecordSet(d.ctx, rs, 8)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	jobBinary := rows[0].GetBytes(0)
	job := &model.Job{}
	err = job.Decode(jobBinary)
	if err != nil {
		return nil, err
	}
	if job.Type == model.ActionDropSchema {
		for {
			canRun, err := d.checkDropSchemaJobIsRunnable(sess, job)
			if err != nil {
				return nil, err
			}
			if canRun {
				return job, nil
			}
			runningOrBlockedIDs = append(runningOrBlockedIDs, strconv.Itoa(int(job.ID)))
			sql = fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where job_id = (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id having not reorg and job_id not in (%s) order by min(job_id) limit 1)", strings.Join(runningOrBlockedIDs, ", "))
			rs, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
			if err != nil {
				return nil, err
			}
			rows, err = sqlexec.DrainRecordSet(d.ctx, rs, 8)
			if err != nil {
				return nil, err
			}
			if len(rows) == 0 {
				return nil, nil
			}
			jobBinary = rows[0].GetBytes(0)
			job = &model.Job{}
			err = job.Decode(jobBinary)
			if err != nil {
				return nil, err
			}
		}
	}
	return job, nil
}

func (d *ddl) checkDropSchemaJobIsRunnable(sess sessionctx.Context, job *model.Job) (bool, error) {
	sql := fmt.Sprintf("select * from mysql.tidb_ddl_job where schema_id = %d and reorg and job_id < %d limit 1", job.SchemaID, job.ID)
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return false, err
	}
	rows, err := sqlexec.DrainRecordSet(d.ctx, rs, 8)
	if err != nil {
		return false, err
	}
	return len(rows) == 0, nil
}

func (d *ddl) checkReorgJobIsRunnable(sess sessionctx.Context, job *model.Job) (bool, error) {
	sql := fmt.Sprintf("select * from mysql.tidb_ddl_job where schema_id = %d and is_drop_schema and job_id < %d limit 1", job.SchemaID, job.ID)
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return false, err
	}
	rows, err := sqlexec.DrainRecordSet(d.ctx, rs, 8)
	if err != nil {
		return false, err
	}
	return len(rows) == 0, nil
}

func (d *ddl) getOneReorgJob(sess sessionctx.Context) (*model.Job, error) {
	sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "begin")
	defer func() {
		sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "commit")
	}()
	runningOrBlockedIDs := make([]string, 0, 10)
	for id := range d.runningReorgJobMap {
		runningOrBlockedIDs = append(runningOrBlockedIDs, strconv.Itoa(id))
	}
	var sql string
	if len(runningOrBlockedIDs) == 0 {
		sql = "select job_meta from mysql.tidb_ddl_job where job_id = (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id having reorg and job_id order by min(job_id) limit 1)"
	} else {
		sql = fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where job_id = (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id having reorg and job_id not in (%s) order by min(job_id) limit 1)", strings.Join(runningOrBlockedIDs, ", "))
	}
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return nil, err
	}
	var rows []chunk.Row
	rows, err = sqlexec.DrainRecordSet(d.ctx, rs, 8)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	jobBinary := rows[0].GetBytes(0)
	job := &model.Job{}
	err = job.Decode(jobBinary)
	if err != nil {
		return nil, err
	}
	for {
		canRun, err := d.checkReorgJobIsRunnable(sess, job)
		if err != nil {
			return nil, err
		}
		if canRun {
			return job, nil
		}
		runningOrBlockedIDs = append(runningOrBlockedIDs, strconv.Itoa(int(job.ID)))
		sql = fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where job_id = (select min(job_id) and not processing from mysql.tidb_ddl_job group by schema_id, table_id having reorg and job_id not in (%s) order by min(job_id) limit 1)", strings.Join(runningOrBlockedIDs, ", "))
		rs, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
		if err != nil {
			return nil, err
		}
		rows, err = sqlexec.DrainRecordSet(d.ctx, rs, 8)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, nil
		}
		jobBinary = rows[0].GetBytes(0)
		job = &model.Job{}
		err = job.Decode(jobBinary)
		if err != nil {
			return nil, err
		}
	}
}

func (d *ddl) startDispatchLoop() {
	for !d.isOwner() {
		time.Sleep(time.Second)
	}
	sess, _ := d.sessPool.get()

	defer d.sessPool.put(sess)
	var notifyDDLJobByEtcdChGeneral clientv3.WatchChan
	var notifyDDLJobByEtcdChReorg clientv3.WatchChan
	if d.etcdCli != nil {
		notifyDDLJobByEtcdChGeneral = d.etcdCli.Watch(context.Background(), addingDDLJobGeneral)
		notifyDDLJobByEtcdChReorg = d.etcdCli.Watch(context.Background(), addingDDLJobReorg)
	}
	ticker := time.NewTicker(1*time.Second)
	var ok bool
	for {
		select {
		case <-d.ddlJobCh:
			sleep := false
			for !sleep {
				job, _ := d.getOneGeneralJob(sess)
				if job != nil {
					d.runningReorgJobMap[int(job.ID)] = struct{}{}
					go func() {
						wk, _ := d.gwp.get()
						wk.handleDDLJob(d.ddlCtx, job, d.ddlJobCh)
						d.gwp.put(wk)
						delete(d.runningReorgJobMap, int(job.ID))
					}()
				}
				job2, _ := d.getOneReorgJob(sess)
				if job2 != nil {
					d.runningReorgJobMap[int(job2.ID)] = struct{}{}
					go func() {
						defer delete(d.runningReorgJobMap, int(job2.ID))
						wk, _ := d.wp.get()
						wk.handleDDLJob(d.ddlCtx, job2, d.ddlJobCh)
						d.wp.put(wk)
					}()
				}
				if job == nil && job2 == nil {
					sleep = true
				}
			}
		case <-ticker.C:
			sleep := false
			for !sleep {
				job, _ := d.getOneGeneralJob(sess)
				if job != nil {
					d.runningReorgJobMap[int(job.ID)] = struct{}{}
					go func() {
						wk, _ := d.gwp.get()
						defer delete(d.runningReorgJobMap, int(job.ID))
						wk.handleDDLJob(d.ddlCtx, job, d.ddlJobCh)
						d.gwp.put(wk)
					}()
				}
				job2, _ := d.getOneReorgJob(sess)
				if job2 != nil {
					d.runningReorgJobMap[int(job2.ID)] = struct{}{}
					go func() {
						defer delete(d.runningReorgJobMap, int(job2.ID))
						wk, _ := d.wp.get()
						wk.handleDDLJob(d.ddlCtx, job2, d.ddlJobCh)
						d.wp.put(wk)
					}()
				}
				if job == nil && job2 == nil {
					sleep = true
				}
			}
		case _, ok = <-notifyDDLJobByEtcdChGeneral:
			if !ok {
				panic("111")
			}
			job, _ := d.getOneGeneralJob(sess)
			if job != nil {
				d.runningReorgJobMap[int(job.ID)] = struct{}{}
				go func() {
					defer delete(d.runningReorgJobMap, int(job.ID))
					wk, _ := d.gwp.get()
					wk.handleDDLJob(d.ddlCtx, job, d.ddlJobCh)
					d.gwp.put(wk)
				}()
			}
		case _, ok = <-notifyDDLJobByEtcdChReorg:
			if !ok {
				panic("2222")
			}
			job, _ := d.getOneReorgJob(sess)
			if job != nil {
				d.runningReorgJobMap[int(job.ID)] = struct{}{}
				go func() {
					defer delete(d.runningReorgJobMap, int(job.ID))
					wk, _ := d.wp.get()
					wk.handleDDLJob(d.ddlCtx, job, d.ddlJobCh)
					d.wp.put(wk)
				}()
			}
		case <-d.ctx.Done():
			return
		}

	}
}

func (d *ddl) addDDLJob(job *model.Job) error {
	b, err := job.Encode(true)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("insert into mysql.tidb_ddl_job values(%d, %t, %d, %d, 0x%x, %d, %t)", job.ID, admin.MayNeedBackfill(job.Type), job.SchemaID, job.TableID, b, 0, job.Type == model.ActionDropSchema)
	log.Warn("add ddl job to table", zap.String("sql", sql))
	sess, _ := d.sessPool.get()
	defer d.sessPool.put(sess)
	_, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) deleteDDLJob(job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_job where job_id = %d", job.ID)
	_, err := w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) updateDDLJobNew(job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("update mysql.tidb_ddl_job set job_meta = 0x%x where job_id = %d", b, job.ID)
	_, err = w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) UpdateDDLReorgStartHandleNew(job *model.Job, element *meta.Element, startKey kv.Key) error {
	sql := fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, curr_ele_id, curr_ele_type) values (%d, %d, 0x%x)", job.ID, element.ID, element.TypeKey)
	_, err := w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, ele_id, start_key) values (%d, %d, 0x%s)", job.ID, element.ID, wrapKey2String(startKey))
	_, err = w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) UpdateDDLReorgHandle(job *model.Job, startKey, endKey kv.Key, physicalTableID int64, element *meta.Element, newSess bool) error {
	sess := w.sessForJob
	if newSess {
		sess, _ = w.sessPool.get()
		defer w.sessPool.put(sess)
		sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "begin")
		defer sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "commit")
	}
	sql := fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, curr_ele_id, curr_ele_type) values (%d, %d, 0x%x)", job.ID, element.ID, element.TypeKey)
	_, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, ele_id, start_key, end_key, physical_id) values (%d, %d, %s, %s, %d)", job.ID, element.ID, wrapKey2String(startKey), wrapKey2String(endKey), physicalTableID)
	_, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) RemoveDDLReorgHandle(job *model.Job, elements []*meta.Element) error {
	if len(elements) == 0 {
		return nil
	}
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	_, err := w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func wrapKey2String(key []byte) string {
	if len(key) == 0 {
		return "''"
	}
	return fmt.Sprintf("0x%x", key)
}
