#requires -version 2
<#set-executionpolicy remotesigned localmachine -f
powershell.exe[ -nol -nop -noni -ex bypass]
  -f activate_standby.ps1[ -activate][ -call]#>
param([switch]$activate,[switch]$call)
set-strictmode -v latest
&{
  function run_sqlp{
    $scr='{0}{1}exit{1}' -f $scr,$my.LNW
    $scr|&$sqlp -sl /nolog|log
    if($lastexitcode -ne 0){
      throw "Ошибка '${lastexitcode}' выполнения 'sqlplus.exe'"
    }
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
    $sqlp=gcm sqlplus.exe
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
            $om=@('READ WRITE','READ ONLY','MOUNTED')
            $pr=chk_db_pars $proc CURRENT PRIMARY $om
            log "Подключение к 'STANDBY' БД..."
            dispose-after($sboc=mk_oc $cs){
              log '... Выполнено'
              $sb=chk_db_pars $sboc STANDBY 'PHYSICAL STANDBY' @('MOUNTED')
              cmp_db_pars
            }
          }
        }
      }
    }
    $scr=@'
whenever oserror exit failure rollback;
whenever sqlerror exit sql.sqlcode rollback;
conn /@primary_ekr as sysdba;
alter system set audit_trail=os scope=spfile;
shutdown immediate;
startup restrict open read only;
alter system archive log current noswitch;
whenever oserror continue none;
whenever sqlerror continue none;
shutdown immediate;
whenever oserror exit failure rollback;
whenever sqlerror exit sql.sqlcode rollback;
startup mount;
'@
    log $scr
    if($activate){
      run_sqlp
      .\recover_standby.ps1 -recover -call
    }
    $scr=@'
whenever oserror exit failure rollback;
whenever sqlerror exit sql.sqlcode rollback;
conn /as sysdba;
alter database activate standby database;
alter system set audit_trail=db,extended scope=spfile;
whenever oserror continue none;
whenever sqlerror continue none;
shutdown immediate;
whenever oserror exit failure rollback;
whenever sqlerror exit sql.sqlcode rollback;
startup;
select controlfile_type,database_role,open_mode from v$database;
conn /@primary_ekr as sysdba;
alter database convert to physical standby;
whenever oserror continue none;
whenever sqlerror continue none;
shutdown immediate;
whenever oserror exit failure rollback;
whenever sqlerror exit sql.sqlcode rollback;
startup mount;
set serveroutput on size 1000000;
declare
  sqls varchar2(1024);
begin
  for r in(select to_char(group#) g from v$log order by group#) loop
    sqls:='alter database clear logfile group '||r.g;
    dbms_output.put_line(sqls||';');
    execute immediate sqls;
  end loop;
end;
/
select controlfile_type,database_role,open_mode from v$database;
'@
    log $scr
    if($activate){run_sqlp}
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
