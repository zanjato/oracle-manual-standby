#requires -version 2
#set-executionpolicy remotesigned localmachine -f
#get-acl (join-path $env:systemroot\system32\tasks backup_oracle.job)|
#  set-acl (join-path $env:systemroot\system32\tasks manual_standby_recovery.job)
#powershell.exe -nol -nop -non ^
#  -f C:\oracle\product\10.2.0\admin\EKR\standby\manual_standby_recovery.ps1
param([switch]$next,[switch]$call)
set-strictmode -vers latest
&{
  function dispose(
    [parameter(mandatory=$true,valuefrompipeline=$true)]$obj){
    process{
      if($obj -is [idisposable] -or $obj -as [idisposable]){
        [idisposable].getmethod('Dispose').invoke($obj,$null)
      }
    }
  }
  function dispose-after($obj,[scriptblock]$sb){try{&$sb}finally{dispose $obj}}
  function log(
    [parameter(mandatory=$true,valuefrompipeline=$true)][string]$log){
    process{
      $log.replace($NWLN,$NL).split($NL,$REE)|
      %{$i=0}{"$(&$lnbg[$i]) -- $_";$i=1}|write-host
    }
  }
  function mk_oc($cs){
    $oc=new-object oracle.dataaccess.client.oracleconnection $cs
    $oc.open()
    $oc
  }
  function chk_db_pars($oc,$cft,$dbr,$om){
    log  "� '${dbr}' �� ��������� ���������� �� V`$DATABASE..."
    $cm.connection=$oc
    $cm.commandtext=@'
select dbid,controlfile_type cft,database_role dbr,log_mode lm,
       open_mode om,resetlogs_change# rl,prior_resetlogs_change# prl
from v$database
'@
    if($da.fill($tbl) -ne 1){throw "������ �� '$($cm.commandtext)'"}
    $r1=$tbl.rows[0]
    log '... ���������'
    log '�������� ���� ������������ �����...'
    if($r1.cft -ne $cft){
      throw "��� ������������ ����� �� '${cft}', � '$($r1.cft)'"
    }
    log '... ���������'
    log '�������� ���� ��...'
    if($r1.dbr -ne $dbr){
      throw "���� �� �� '${dbr}', � '$($r1.dbr)'"
    }
    log '... ���������'
    log '�������� ������ ������������� ��������...'
    if($r1.lm -ne 'ARCHIVELOG'){
      throw "����� ������������� �������� �� 'ARCHIVELOG', � '$($r1.lm)'"
    }
    log '... ���������'
    log '�������� ������ �������� ��...'
    if(@($om) -notcontains $r1.om ){
      throw "����� �������� �� �� '${om}', � '$($r1.om)'"
    }
    $r1.dbid,$r1.rl,$r1.prl
    $tbl.reset()
    log '... ���������'
  }
  function pr_dst{
    log @'
� 'PRIMARY' �� ��������� DEST_ID ��� 'LOG_ARCHIVE_DEST_1' �� V$ARCHIVE_DEST...
'@
    $cm.connection=$proc
    $cm.commandtext=@'
select dest_id
  from v$archive_dest
 where dest_name = 'LOG_ARCHIVE_DEST_1'
   and status = 'VALID'
   and binding = 'MANDATORY'
   and name_space = 'SYSTEM'
   and target = 'PRIMARY'
   and archiver = 'ARCH'
   and schedule = 'ACTIVE'
   and upper(destination) <> 'USE_DB_RECOVERY_FILE_DEST'
   and process = 'ARCH'
   and register = 'YES'
   and alternate = 'NONE'
   and dependency = 'NONE'
   and remote_template = 'NONE'
   and transmit_mode = 'SYNCHRONOUS'
   and affirm = 'NO'
   and type = 'PUBLIC'
   and valid_now = 'YES'
   and valid_type = 'ALL_LOGFILES'
   and valid_role = 'ALL_ROLES'
   and db_unique_name = 'NONE'
'@
    if($da.fill($tbl) -ne 1){
      throw @'
� 'PRIMARY' �� 'LOG_ARCHIVE_DEST_1' �� ��������� ��� ��������� � 'USE_DB_RECOVERY_FILE_DEST'
'@
    }
    $tbl.rows[0].dest_id
    $tbl.reset()
    log '... ���������'
  }
  function pr_th{
    $msg="� 'PRIMARY' �� ��������� V`$INSTANCE.THREAD#"
    log "${msg}..."
    $cm.commandtext=@'
select min(thread#) th
  from v$instance i
     , (select to_number(value) instance_nbr
          from v$parameter
         where lower(name) = 'instance_number') b
     , (select value instance_name
          from v$parameter
         where lower(name) = 'instance_name') a
 where case b.instance_nbr
       when 0 then i.instance_name
       else to_char(i.instance_number) end
     = case b.instance_nbr
       when 0 then a.instance_name
       else to_char(b.instance_nbr) end
'@
    if($da.fill($tbl) -ne 1){throw $msg}
    $tbl.rows[0].th
    $tbl.reset()
    log '... ���������'
  }
  function chk_db_ids{
    log '��������� ��������������� ��� ������...'
    if($prid -ne $sbid){
      throw @"
�������������� 'PRIMARY' �� (${prid}) � 'STANDBY' �� (${sbid}) �� �����
"@
    }
    log '... ���������'
  }
  function sb_cn{
    $msg="� 'STANDBY' �� ��������� SCN"
    log "${msg}..."
    $cm.connection=$sboc
    $cm.commandtext=@'
select min(scn) scn
  from (select checkpoint_change# scn from v$datafile_header
         union all
        select checkpoint_change# from v$datafile
         union all
        select current_scn
/*               case
                 when current_scn is null
                   or current_scn > checkpoint_change#
                 then checkpoint_change#
                 else current_scn
               end*/
          from v$database
         where current_scn > 0)
'@
    if($da.fill($tbl) -ne 1){throw $msg}
    $tbl.rows[0].scn
    $tbl.reset()
    log '... ���������'
  }
  function chk_db_incs{
    log '�������� ���������� ��� ������...'
    if(($prrl -ne $sbrl -or $prprl -ne $sbprl) -and
       ($prprl -ne $sbrl -or @(($sbcn+2),($sbcn+3)) -notcontains $prrl)){
      throw '���������� ��� ������ �� ����������'
    }
    log '... ���������'
  }
  function sb_dst{
    log "� 'STANDBY' �� ��������� 'STANDBY_ARCHIVE_DEST' �� V`$ARCHIVE_DEST..."
    $cm.commandtext=@'
select destination
  from v$archive_dest
 where dest_name = 'STANDBY_ARCHIVE_DEST'
   and status = 'VALID'
   and binding = 'MANDATORY'
   and name_space = 'SYSTEM'
   and target = 'LOCAL'
   and archiver = 'ARCH'
   and schedule = 'ACTIVE'
   and process = 'RFS'
   and register = 'NO'
   and alternate = 'NONE'
   and dependency = 'NONE'
   and remote_template = 'NONE'
   and transmit_mode = 'SYNCHRONOUS'
   and affirm = 'NO'
   and type = 'PUBLIC'
   and valid_now = 'YES'
   and valid_type = 'ALL_LOGFILES'
   and valid_role = 'ALL_ROLES'
   and db_unique_name = 'NONE'
'@
    if($da.fill($tbl) -ne 1){
      throw "� 'STANDBY' �� 'STANDBY_ARCHIVE_DEST' �� ���������"
    }
    $sbad=$tbl.rows[0].destination.trimend('\')
    $sbad
    $tbl.reset()
    log '... ���������'
  }
  function pr_arcs{
    $cm.connection=$proc
    if($next){
      log "� 'PRIMARY' �� ������������ ����������� �����..."
      $cm.commandtext=@'
begin execute immediate 'alter system archive log current';end;
'@ -replace $NWLN,$NL
      [void]$cm.executenonquery()
      log '... ���������'
    }
    log "� 'PRIMARY' �� ��������� ������ �������� ���������� ������..."
    $cm.commandtext=@"
select name,next_change# xcn
from v`$archived_log
where name is not null and dest_id = ${prd1} and thread# = ${prth}
--  and resetlogs_change# = (select resetlogs_change# from v`$database)
  and resetlogs_id = (select resetlogs_id
                      from v`$database_incarnation
                      where status = 'CURRENT')
  and (first_change# > ${sbcn} or next_change# > ${sbcn} + 1)
  and deleted = 'NO' and status = 'A' and is_recovery_dest_file = 'NO'
"@
    if($da.fill($tbl) -lt 1){
      throw "� 'PRIMARY' �� ��� �������� ���������� ������ ��� �����������"
    }
<#    $bf=[reflection.bindingflags]'nonpublic,instance'
    $mwc=([array]$tbl.rows).gettype().getmethod('MemberwiseClone',$bf)
    $arcs=$mwc.invoke([array]$tbl.rows,$null)#>
    $tbl|%{$arcs=@{}}{$arcs[$_.name]=$_.xcn}{$arcs}
    $tbl.reset()
    log '... ���������'
  }
  function cp_arcs{
    log '����������� � ����������� �������� ���������� ������...'
    $cm.commandtext="select sys_context('USERENV','SERVER_HOST') svr from dual"
    if($da.fill($tbl) -ne 1){throw "������ �� '$($cm.commandtext)'"}
    $svr=[net.dns]::gethostentry($tbl.rows[0].svr).hostname
    $cm.connection=$sboc
    $arcs.keys|%{$xcn=0}{
      if($xcn -lt $arcs[$_]){$xcn=$arcs[$_]}
      $arc=split-path $_ -le
      $_="\\${svr}\archivedlogs\${arc}"
      log "����������� '$_' � '${sbad}'..."
      cp -l $_ $sbad
      log '... ���������'
      $_="${sbad}\${arc}"
      log "����������� '$_'..."
      $cm.commandtext=@"
declare full_name varchar2(513);recid number;stamp number;
begin dbms_backup_restore.inspectArchivedLog('$_',full_name,recid,stamp);end;
"@ -replace $NWLN,$NL
      [void]$cm.executenonquery()
      log '... ���������'
    }
    $xcn
    $tbl.reset()
  }
  function sb_rcvr{
    $sql=@"
alter database recover automatic
from '${sbad}'
standby database until change ${xcn}
"@
    log "� 'STANDBY' �� ���������� '${sql}'..."
    $sql=$sql -replace "'","''" -replace $NWLN,$NL
    $cm.commandtext="begin execute immediate '${sql}'; end;"
    [void]$cm.executenonquery()
    log '... ���������'
  }
  function sb_chk_cn{
    log "� 'STANDBY' �� �������� V`$DATABASE.CURRENT_SCN..."
    $cm.commandtext='select current_scn from v$database'
    if($da.fill($tbl) -ne 1){throw "�������� SCN � 'STANDBY' ��"}
    $ccn=$tbl.rows[0].current_scn+1
    if($xcn -ne $ccn){
      throw "� 'STANDBY' �� V`$DATABASE.CURRENT_SCN �� '${xcn}', � '${ccn}'"
    }
    log '... ���������'
  }
  $sw=[diagnostics.stopwatch]::startnew()
  $erroractionpreference='stop'
  $props=@{tran=$null}
  try{
    $dt=date
    $dt='{0}_{1:HHmm}' -f (($dt-[datetime]0).days%7+1),$dt
    $sn=$myinvocation.scriptname
    $log=[io.path]::getfilenamewithoutextension($sn)
    $log="$(split-path $sn)\logs\${log}_${dt}.log"
    try{
      start-transcript $log -f -outv tran|write-host
      $props.tran=$tran
    }catch{}
    $NWLN=[environment]::newline
    $NL=[char[]]"`n"
    $REE=[stringsplitoptions]::removeemptyentries
    $lnbg={date -f 'yyyy-MM-dd HH:mm:ss.fffffff'},{' '*27}
    if(!$call){log (gwmi win32_process -f "handle=${pid}").commandline}
    if(!$env:oracle_sid.trim()){throw '��� ORACLE_SID'}
    [void][reflection.assembly]::loadwithpartialname('Oracle.DataAccess')
    log "����������� � 'PRIMARY' ��..."
    $cs='user id=/;dba privilege=sysdba'
    dispose-after($proc=mk_oc "data source=primary_ekr;${cs}"){
      log '... ���������'
      dispose-after($cm=new-object oracle.dataaccess.client.oraclecommand){
        dispose-after(
          $da=new-object oracle.dataaccess.client.oracledataadapter){
          $da.selectcommand=$cm
          dispose-after($tbl=new-object data.datatable){
            $om=,'READ WRITE'
            if(!$next){$om+='MOUNTED','READ ONLY'}
            $prid,$prrl,$prprl=chk_db_pars $proc CURRENT PRIMARY $om
            $prd1=pr_dst
            $prth=pr_th
            log "����������� � 'STANDBY' ��..."
            dispose-after($sboc=mk_oc $cs){
              log '... ���������'
              $sbid,$sbrl,$sbprl=chk_db_pars $sboc STANDBY 'PHYSICAL STANDBY' `
                                             MOUNTED
              chk_db_ids
              $sbcn=sb_cn
              chk_db_incs
              $sbad=sb_dst
              $arcs=pr_arcs
              $xcn=cp_arcs
              sb_rcvr
              sb_chk_cn
            }
          }
        }
      }
    }
    if(!$call){log "��������� '$($sw.elapsed)'";exit 0}
  }catch{
    if($call){throw}else{$_|out-string|write-warning|write-host;exit 1}
  }finally{
    if($props.tran){stop-transcript|write-host}
  }
}