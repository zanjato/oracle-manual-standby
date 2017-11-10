#requires -version 2
<#set-executionpolicy remotesigned localmachine -f
powershell.exe -c get-acl backup_oracle.job^|set-acl recover_standby.job
cmd.exe /c powershell.exe[ -nol -nop -noni -ex bypass]
  -f recover_standby.ps1[ -recover][ -switch][ -call]#>
param([switch]$recover,[switch]$switch,[switch]$call)
set-strictmode -v latest
&{
  function pr_dst{
    log @'
В 'PRIMARY' БД получение DEST_ID для 'LOG_ARCHIVE_DEST_1' из V$ARCHIVE_DEST...
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
   and dependency = 'NONE'
   and type = 'PUBLIC'
   and valid_now = 'YES'
   and valid_type = 'ALL_LOGFILES'
   and valid_role = 'ALL_ROLES'
'@
    if($da.fill($tbl) -ne 1){
      throw @'
В 'PRIMARY' БД 'LOG_ARCHIVE_DEST_1' не определен или совпадает с 'USE_DB_RECOVERY_FILE_DEST'
'@
    }
    $tbl.rows[0].dest_id
    $tbl.reset()
    log '... Выполнено'
  }
  function sb_cn{
    $msg="В 'STANDBY' БД получение SCN"
    log "${msg}..."
    $cm.connection=$sboc
    $cm.commandtext='select current_scn scn from v$database'
    if($da.fill($tbl) -ne 1 -or
       ($scn=$tbl.rows[0].scn) -eq $null -or
       $scn -le 0){throw $msg}
    $scn
    $tbl.reset()
    log '... Выполнено'
  }
  function sb_dst{
    log "В 'STANDBY' БД получение 'STANDBY_ARCHIVE_DEST' из V`$ARCHIVE_DEST..."
    $cm.connection=$sboc
    $cm.commandtext=@'
select destination
  from v$archive_dest
 where dest_name = 'STANDBY_ARCHIVE_DEST'
   and name_space = 'SYSTEM'
   and type = 'PUBLIC'
'@
    if($da.fill($tbl) -ne 1){
      throw "В 'STANDBY' БД 'STANDBY_ARCHIVE_DEST' не определен"
    }
    $tbl.rows[0].destination.trimend('\')
    $tbl.reset()
    log '... Выполнено'
  }
  function pr_arcs{
    $cm.connection=$proc
    if($switch){
      log "В 'PRIMARY' БД переключение журнального файла..."
      $cm.commandtext=@'
begin execute immediate 'alter system archive log current';end;
'@ -replace $my.LNW,$my.NL.str
      if($recover){[void]$cm.executenonquery()}
      log '... Выполнено'
    }
    log "В 'PRIMARY' БД получение списка архивных журнальных файлов..."
    $rltm=$pr.rltm.tostring('yyyyMMddHHmmss')
    $cm.commandtext=@"
select name,next_change# xcn
from v`$archived_log a join v`$instance i on a.thread#=i.thread#
where a.name is not null and a.dest_id=${prd1} and
  a.is_recovery_dest_file='NO' and a.resetlogs_change#=$($pr.rlch) and
  a.resetlogs_time=to_date('${rltm}','yyyymmddhh24miss') and
  a.resetlogs_id=$($pr.rlid) and a.deleted='NO' and a.status='A' and
  ${sbcn}<a.next_change#-1
"@
    if($da.fill($tbl) -le 0){
      throw "В 'PRIMARY' БД нет архивных журнальных файлов для копирования"
    }
    $arcs=@{}
    $tbl|%{$arcs[$_.name]=$_.xcn}
    $arcs
    $tbl.reset()
    log '... Выполнено'
  }
  function cp_arcs{
    log 'Копирование и регистрация архивных журнальных файлов...'
    $cm.connection=$proc
    $cm.commandtext="select sys_context('USERENV','SERVER_HOST') svr from dual"
    if($da.fill($tbl) -ne 1){throw "Чтение из '$($cm.commandtext)'"}
    $svr=[net.dns]::gethostentry($tbl.rows[0].svr).hostname
    $cm.connection=$sboc
    $arcs.keys|%{$xcn=0}{
      if($xcn -lt $arcs[$_]){$xcn=$arcs[$_]}
      $arc=split-path $_ -le
      $_="\\${svr}\archivedlogs\${arc}"
      log "Копирование '$_' в '${sbad}'..."
      if($recover){cp -l $_ $sbad}
      log '... Выполнено'
      $_="${sbad}\${arc}"
      log "Регистрация '$_'..."
      $cm.commandtext=@"
declare arcnm varchar2(513);rid number;stamp number;
begin dbms_backup_restore.inspectArchivedLog('$_',arcnm,rid,stamp);end;
"@ -replace $my.LNW,$my.NL.str
      if($recover){[void]$cm.executenonquery()}
      log '... Выполнено'
    }
    $xcn
    $tbl.reset()
  }
  function rcv_sb{
    $sql=@"
alter database recover automatic from '${sbad}' standby database until change ${xcn}
"@
    log "В 'STANDBY' БД выполнение '${sql}'..."
    $sql=$sql -replace "'","''" -replace $my.LNW,$my.NL.str
    $cm.connection=$sboc
    $cm.commandtext="begin execute immediate '${sql}'; end;"
    if($recover){[void]$cm.executenonquery()}
    log '... Выполнено'
  }
  function chk_sb_cn{
    $ccn=sb_cn
    log "В 'STANDBY' БД проверка V`$DATABASE.CURRENT_SCN..."
    if(--$xcn -ne $ccn){
      throw "В 'STANDBY' БД V`$DATABASE.CURRENT_SCN не '${xcn}', а '${ccn}'"
    }
    log '... Выполнено'
  }
  $sw=[diagnostics.stopwatch]::startnew()
  $erroractionpreference='stop'
  try{
    'aux','ora'|%{. .\${_}_utils.ps1}
    $my=set_my
    set_bsw
    mk_log
    if(!$call){log (gwmi win32_process -f "handle=${pid}").commandline}
    if(!($env:oracle_sid|? $my.CE)){throw 'Нет ORACLE_SID'}
    [void][reflection.assembly]::loadwithpartialname('Oracle.DataAccess')
    log "Подключение к 'PRIMARY' БД..."
    $cs='user id=/;dba privilege=sysdba'
    dispose-after($proc=mk_oc "data source=primary_ekr;${cs}"){
      log '... Выполнено'
      dispose-after($cm=new-object oracle.dataaccess.client.oraclecommand){
        dispose-after(
          $da=new-object oracle.dataaccess.client.oracledataadapter){
          $da.selectcommand=$cm
          dispose-after($tbl=new-object data.datatable){
            $om=@('READ WRITE')
            if(!$switch){$om+='READ ONLY','MOUNTED'}
            $pr=chk_db_pars $proc CURRENT PRIMARY $om
            $prd1=pr_dst
            log "Подключение к 'STANDBY' БД..."
            dispose-after($sboc=mk_oc $cs){
              log '... Выполнено'
              $sb=chk_db_pars $sboc STANDBY 'PHYSICAL STANDBY' @('MOUNTED')
              cmp_db_pars
              $sbcn=sb_cn
              $sbad=sb_dst
              $arcs=pr_arcs
              $xcn=cp_arcs
              rcv_sb
              chk_sb_cn
            }
          }
        }
      }
    }
    if(!$call){log "Затрачено '$($sw.elapsed)'";exit 0}
  }catch{
    if($call){throw}
    else{
      $x=$_|out-string
      if(test-path variable:my){$x|log -err}else{$x|write-warning}
      exit 1
    }
  }finally{
    if(test-path variable:my){
      if($my.tran){stop-transcript >$null}
      if($my.bsw){set_bsw $my.bsw}
    }
  }
}
