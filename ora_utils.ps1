function chk_db_pars{
  param([validatenotnull()][oracle.dataaccess.client.oracleconnection]$oc,
        [validateset('CURRENT','STANDBY')][string]$cft,
        [validateset('PRIMARY','PHYSICAL STANDBY')][string]$dbrl,
        [validatenotnullorempty()][string[]]$om)
  log "В '${dbrl}' БД получение информации из V`$DATABASE..."
  $cm.connection=$oc
  $cm.commandtext=@'
select d.dbid,d.name dbnm,d.created dbcr,
  d.resetlogs_change# rlch,d.resetlogs_time rltm,
  d.log_mode lm,d.controlfile_type cft,d.open_mode om,
  d.database_role dbrl,i.incarnation# inbr,i.status inst
'@
  if($dbrl -eq 'PRIMARY'){
    $cm.commandtext+=@"
,$($my.LNW)  d.prior_resetlogs_change# prlch,d.prior_resetlogs_time prltm,
  i.resetlogs_id rlid$($my.LNW)
"@
  }
  $cm.commandtext+=@'
from v$database d join v$database_incarnation i
  on d.resetlogs_change#=i.resetlogs_change# and
     d.resetlogs_time=i.resetlogs_time
'@
  if($da.fill($tbl) -ne 1){throw "Чтение из '$($cm.commandtext)'"}
  $r1=$tbl.rows[0]
  log '... Выполнено'
  log 'Проверка режима архивирования журналов...'
  if($r1.lm -ne 'ARCHIVELOG'){
    throw "Режим архивирования журналов не 'ARCHIVELOG', а '$($r1.lm)'"
  }
  log '... Выполнено'
  log 'Проверка типа управляющего файла...'
  if($r1.cft -ne $cft){
    throw "Тип управляющего файла не '${cft}', а '$($r1.cft)'"
  }
  log '... Выполнено'
  log 'Проверка режима открытия БД...'
  if($om -notcontains $r1.om ){
    throw "Режим открытия БД не '${om}', а '$($r1.om)'"
  }
  log '... Выполнено'
  log 'Проверка роли БД...'
  if($r1.dbrl -ne $dbrl){
    throw "Роль БД не '${dbrl}', а '$($r1.dbrl)'"
  }
  log '... Выполнено'
  log 'Проверка статуса инкарнации БД...'
  if($r1.inst -ne 'CURRENT'){
    throw "Статус инкарнации БД не 'CURRENT', а '$($r1.inst)'"
  }
  $pars=@{}
  $props='dbid','dbnm','dbcr','rlch','rltm','inbr'
  if($dbrl -eq 'PRIMARY'){$props+='prlch','prltm','rlid'}
  $props|%{$pars[$_]=$r1.$_}
  $pars
  $tbl.reset()
  log '... Выполнено'
}
function cmp_db_pars{
  log 'Сравнение идентификаторов баз данных...'
  if($pr.dbid -ne $sb.dbid){
    throw @"
Идентификаторы 'PRIMARY' БД ($($pr.dbid)) и 'STANDBY' БД ($($sb.dbid)) не равны
"@
  }
  log '... Выполнено'
  log 'Сравнение имен баз данных...'
  if($pr.dbnm -ne $sb.dbnm){
    throw @"
Имена 'PRIMARY' БД ($($pr.dbnm)) и 'STANDBY' БД ($($sb.dbnm)) не совпадают
"@
  }
  log '... Выполнено'
  log 'Сравнение дат создания баз данных...'
  if($pr.dbcr -ne $sb.dbcr){
    $prcr,$sbcr=$pr,$sb|%{$_.dbcr.tostring('yyyy-MM-dd HH:mm:ss')}
    throw @"
Даты создания 'PRIMARY' БД (${prcr}) и 'STANDBY' БД (${sbcr}) не совпадают
"@
  }
  log '... Выполнено'
  log 'Сравнение инкарнаций баз данных...'
  if(($pr.rlch -ne $sb.rlch -or
      $pr.rltm -ne $sb.rltm -or
      $pr.inbr -ne $sb.inbr) -and
     ($pr.prlch -ne $sb.rlch -or
      $pr.prltm -ne $sb.rltm -or
      $pr.inbr-1 -ne $sb.inbr)){
    throw "Инкарнации 'PRIMARY' БД и 'STANDBY' БД не совпадают"
  }
  log '... Выполнено'
}
