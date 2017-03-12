#requires -version 2
<#set-executionpolicy remotesigned localmachine -f
get-acl (join-path $env:windir\system32\tasks backup_oracle.job)|
  set-acl (join-path $env:windir\system32\tasks manual_standby_recovery.job)
powershell.exe -nol -nop -non -f manual_standby_recovery.ps1[ -next][ -call]#>
param([switch]$next,[switch]$call)
set-strictmode -vers latest
&{
  function chkpars{
    param([parameter(mandatory=$true)][validatenotnull()]
          [management.automation.commandinfo]$ci,
          [parameter(mandatory=$true)][validatenotnull()]
          [collections.generic.dictionary[string,object]]$bndpars)
    $ci.parameters.getenumerator()|?{!$_.value.switchparameter}|
    %{gv $_.key -ea silentlycontinue}|?{!$bndpars.containskey($_.name)}|
    %{throw "Функция '$($ci.name)' вызвана без параметра '$($_.name)'"}
  }
  function dispose-after{[cmdletbinding()]
    param([validatenotnull()][object]$obj,[validatenotnull()][scriptblock]$sb)
    chkpars $myinvocation.mycommand $psboundparameters
    try{&$sb}
    finally{
      if($obj -is [idisposable] -or $obj -as [idisposable]){
        [void][idisposable].getmethod('Dispose').invoke($obj,$null)
      }
    }
  }
  function log{[cmdletbinding()]
    param([parameter(valuefrompipeline=$true)]
          [validatenotnullorempty()][string]$log,[switch]$err)
    process{
      chkpars $myinvocation.mycommand $psboundparameters
      $log.replace($LNW,$NL.str).split($NL.ach,$REE)|
      %{$i=0}{"$(&$lnbg[$i]) $(if($err){'!!'}else{'--'}) $_";$i=1}
    }
  }
  function mk_oc{[cmdletbinding()]param([validatenotnullorempty()][string]$cs)
    chkpars $myinvocation.mycommand $psboundparameters
    $oc=new-object oracle.dataaccess.client.oracleconnection $cs
    $oc.open()
    $oc
  }
  function chk_db_pars{[cmdletbinding()]
      param([validatenotnull()][oracle.dataaccess.client.oracleconnection]$oc,
            [validatenotnullorempty()][string]$cft,
            [validatenotnullorempty()][string]$dbr,
            [validatenotnull()][string[]]$om)
    chkpars $myinvocation.mycommand $psboundparameters
    log "В '${dbr}' БД получение информации из V`$DATABASE..."
    $cm.connection=$oc
    $cm.commandtext=@'
select dbid,controlfile_type cft,database_role dbr,log_mode lm,
       open_mode om,resetlogs_change# rl,prior_resetlogs_change# prl
from v$database
'@
    if($da.fill($tbl) -ne 1){throw "Чтение из '$($cm.commandtext)'"}
    $r1=$tbl.rows[0]
    log '... Выполнено'
    log 'Проверка типа управляющего файла...'
    if($r1.cft -ne $cft){
      throw "Тип управляющего файла не '${cft}', а '$($r1.cft)'"
    }
    log '... Выполнено'
    log 'Проверка роли БД...'
    if($r1.dbr -ne $dbr){
      throw "Роль БД не '${dbr}', а '$($r1.dbr)'"
    }
    log '... Выполнено'
    log 'Проверка режима архивирования журналов...'
    if($r1.lm -ne 'ARCHIVELOG'){
      throw "Режим архивирования журналов не 'ARCHIVELOG', а '$($r1.lm)'"
    }
    log '... Выполнено'
    log 'Проверка режима открытия БД...'
    if($om -notcontains $r1.om ){
      throw "Режим открытия БД не '${om}', а '$($r1.om)'"
    }
    $r1.dbid,$r1.rl,$r1.prl
    $tbl.reset()
    log '... Выполнено'
  }
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
В 'PRIMARY' БД 'LOG_ARCHIVE_DEST_1' не определен или совпадает с 'USE_DB_RECOVERY_FILE_DEST'
'@
    }
    $tbl.rows[0].dest_id
    $tbl.reset()
    log '... Выполнено'
  }
  function pr_th{
    $msg="В 'PRIMARY' БД получение V`$INSTANCE.THREAD#"
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
    log '... Выполнено'
  }
  function chk_db_ids{
    log 'Сравнение идентификаторов баз данных...'
    if($prid -ne $sbid){
      throw @"
Идентификаторы 'PRIMARY' БД (${prid}) и 'STANDBY' БД (${sbid}) не равны
"@
    }
    log '... Выполнено'
  }
  function sb_cn{
    $msg="В 'STANDBY' БД получение SCN"
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
    log '... Выполнено'
  }
  function chk_db_incs{
    log 'Проверка инкарнаций баз данных...'
    if(($prrl -ne $sbrl -or $prprl -ne $sbprl) -and
       ($prprl -ne $sbrl -or @(($sbcn+2),($sbcn+3)) -notcontains $prrl)){
      throw 'Инкарнации баз данных не совместимы'
    }
    log '... Выполнено'
  }
  function sb_dst{
    log "В 'STANDBY' БД получение 'STANDBY_ARCHIVE_DEST' из V`$ARCHIVE_DEST..."
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
      throw "В 'STANDBY' БД 'STANDBY_ARCHIVE_DEST' не определен"
    }
    $sbad=$tbl.rows[0].destination.trimend('\')
    $sbad
    $tbl.reset()
    log '... Выполнено'
  }
  function pr_arcs{
    $cm.connection=$proc
    if($next){
      log "В 'PRIMARY' БД переключение журнального файла..."
      $cm.commandtext=@'
begin execute immediate 'alter system archive log current';end;
'@ -replace $LNW,$NL
      [void]$cm.executenonquery()
      log '... Выполнено'
    }
    log "В 'PRIMARY' БД получение списка архивных журнальных файлов..."
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
      throw "В 'PRIMARY' БД нет архивных журнальных файлов для копирования"
    }
<#    $bf=[reflection.bindingflags]'nonpublic,instance'
    $mwc=([array]$tbl.rows).gettype().getmethod('MemberwiseClone',$bf)
    $arcs=$mwc.invoke([array]$tbl.rows,$null)#>
    $tbl|%{$arcs=@{}}{$arcs[$_.name]=$_.xcn}{$arcs}
    $tbl.reset()
    log '... Выполнено'
  }
  function cp_arcs{
    log 'Копирование и регистрация архивных журнальных файлов...'
    $cm.commandtext="select sys_context('USERENV','SERVER_HOST') svr from dual"
    if($da.fill($tbl) -ne 1){throw "Чтение из '$($cm.commandtext)'"}
    $svr=[net.dns]::gethostentry($tbl.rows[0].svr).hostname
    $cm.connection=$sboc
    $arcs.keys|%{$xcn=0}{
      if($xcn -lt $arcs[$_]){$xcn=$arcs[$_]}
      $arc=split-path $_ -le
      $_="\\${svr}\archivedlogs\${arc}"
      log "Копирование '$_' в '${sbad}'..."
      cp -l $_ $sbad
      log '... Выполнено'
      $_="${sbad}\${arc}"
      log "Регистрация '$_'..."
      $cm.commandtext=@"
declare full_name varchar2(513);recid number;stamp number;
begin dbms_backup_restore.inspectArchivedLog('$_',full_name,recid,stamp);end;
"@ -replace $LNW,$NL
      [void]$cm.executenonquery()
      log '... Выполнено'
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
    log "В 'STANDBY' БД выполнение '${sql}'..."
    $sql=$sql -replace "'","''" -replace $LNW,$NL
    $cm.commandtext="begin execute immediate '${sql}'; end;"
    [void]$cm.executenonquery()
    log '... Выполнено'
  }
  function sb_chk_cn{
    log "В 'STANDBY' БД проверка V`$DATABASE.CURRENT_SCN..."
    $cm.commandtext='select current_scn from v$database'
    if($da.fill($tbl) -ne 1){throw "Проверка SCN в 'STANDBY' БД"}
    $ccn=$tbl.rows[0].current_scn+1
    if($xcn -ne $ccn){
      throw "В 'STANDBY' БД V`$DATABASE.CURRENT_SCN не '${xcn}', а '${ccn}'"
    }
    log '... Выполнено'
  }
  $erroractionpreference='stop'
  try{
    $sw=[diagnostics.stopwatch]::startnew()
    $props=@{tran=$null}
    $dt=date
    $dt='{0}_{1:HHmm}' -f (($dt-[datetime]0).days%7+1),$dt
    $sn=$myinvocation.scriptname
    $log=[io.path]::getfilenamewithoutextension($sn)
    $log="$(split-path $sn)\logs\${log}_${dt}.log"
    $LNW=[environment]::newline
    $NL=@{str="`n";ach=[char[]]"`n"}
    $REE=[stringsplitoptions]::removeemptyentries
    $lnbg={date -f 'yyyy-MM-dd HH:mm:ss.fffffff'},{' '*27}
    try{start-transcript $log -f -outv tran|log;$props.tran=$tran}catch{}
    if(!$call){log (gwmi win32_process -f "handle=${pid}").commandline}
    if(!(test-path env:oracle_sid)){throw 'Нет ORACLE_SID'}
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
            if(!$next){$om+='MOUNTED','READ ONLY'}
            $prid,$prrl,$prprl=chk_db_pars $proc CURRENT PRIMARY $om
            $prd1=pr_dst
            $prth=pr_th
            log "Подключение к 'STANDBY' БД..."
            dispose-after($sboc=mk_oc $cs){
              log '... Выполнено'
              $sbid,$sbrl,$sbprl=chk_db_pars $sboc STANDBY 'PHYSICAL STANDBY' `
                                             @('MOUNTED')
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
    if(!$call){log "Затрачено '$($sw.elapsed)'";exit 0}
  }catch{if($call){throw}else{$_|out-string|log -err;exit 1}}
   finally{if($props.tran){stop-transcript >$null}}
}