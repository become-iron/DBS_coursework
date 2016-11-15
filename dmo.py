# -*- coding: utf-8 -*-
""" Реализация ДМВ в ВСПТД """

# WARN TEMP в настоящий момент работа осуществляется только с SQLite
# WARN выполнение действия НАЙТИ_В_БД потенциально небезопасно

import re
from sqlalchemy import create_engine, Table, MetaData, Column, select, func, and_
from functools import lru_cache
from os.path import isfile as exists_file

from vsptd import Trp, TrpStr, parse_trp_str, check_condition, RE_PREFIX_NAME_NI, RE_PREFIX_NAME_WODS_NI
from ontVoc import trpGetOntVoc

# TEMP временно "вшитые" значения
# таблица в базе метаданных с данными таблиц агентов
_tmp_agents_tbl = 'AGENTS'
_tmp_agents_tbl_c_name = 'NAME'
_tmp_agents_tbl_c_db = 'DB'
# назв. таблицы в базе метаданных  с онтологическим словарём, определяющим характеристики объектов агентов
_tmp_ont_dict_nm = 'OSl_test_1'
# значения имени в триплете, опредляющем значения характеристики для агента
_tmp_agent_trp_nm = 'NAME'
# префикс и имя для выборки инструментов из базы
_tmp_prefix = 'E'
_tmp_name = 'NM'

RE_RULE = re.compile('^ЕСЛИ (.+) ТО (.+);$')  # правило
RE_ACT_FIND_IN_DB = re.compile(r'^НАЙТИ_В_БД\((.*)\)$')  # искать в БД
RE_ACT_FIND_IN_DB_WO = re.compile(r'^НАЙТИ_В_БД\((.*)\\\\(.*)(\+|-)\\\\\)$')  # искать в БД (with order by)
RE_ACT_ADD_IN_DB = re.compile(r'^ДОБАВИТЬ_В_БД\(([A-Za-z]+\d*)\)$')  # добавить в БД
RE_ACT_DEL_FROM_DB = re.compile(r'^УДАЛИТЬ_В_БД\(([A-Za-z]+\d*)\)$')  # удалить из БД


def _get_agent_tbl_cln(prefix, name, ctx):
    """
    ПО ПРЕФИКСУ И ИМЕНИ ТРИПЛЕТА ПОЛУЧИТЬ ИЗ ОНТОЛ. СЛОВАРЯ НАЗВАНИЕ СООТВ. СТОЛБЦА В ТАБЛИЦЕ АГЕНТА
    Обёртка функции trpGetOntVoc для более удобного её использовния
    """
    _ = trpGetOntVoc(prefix, name, ctx.path_mdb, ctx.type_mdb, _tmp_ont_dict_nm)
    _ = _[(ctx.agent, _tmp_agent_trp_nm)]  # альтернатива: [ctx.agent][0].value
    return _


def exec_rule(rule, ctx):
    """
    РАСЧЁТ ПРАВИЛА
    Виды правил:
        - ЕСЛИ УСЛОВИЕ ТО НАЙТИ_В_БД(ПАРАМЕТРЫ);
        - ЕСЛИ УСЛОВИЕ ТО ДОБАВИТЬ_В_БД(ПАРАМЕТРЫ);
        - ЕСЛИ УСЛОВИЕ ТО УДАЛИТЬ_В_БД(ПАРАМЕТРЫ);
    Триплеты в условии правила без "$", заменяются соответствующими значениями, указанными в trp_str_from_db
    Триплеты в параметрах действия правила без "$", заменяются соответствующими значениями из онтологического словаря
    согласно заданному агенту
    Если отсутствует нужда в trp_str и trp_str_from_db в параметре контекста, то их ключам должны соответствовать
    пустые строки
    type_db, type_mdb в параметре контекста должны принимать строго определённые строковые обозначения или числовой
    код. Например: 'SQLite' или 1
    Принимает:
        rule (str) - правило. Например: ЕСЛИ $L.WOB=25 ТО ДОБАВИТЬ_В_БД('E');
        ctx (dict) - метаданные
            trp_str (str/TrpStr) - триплексная строка
            trp_str_from_db (str/TrpStr) - триплексная строка по данным из базы данных
            path_db (str) - путь к базе, к которой будет осуществлён SQL-запрос
            type_db (str) - тип базы данных
            path_mdb (str) - путь к базе, содержащей онтологический словарь метаданных и данные агентов
            type_mdb (str) - тип базы метаданных
            agent (str) - имя агента
    Возвращает:
        НАЙТИ_В_БД(ПАРАМЕТРЫ)
            (TrpStr) - результаты поискового запроса. Вернётся пустая трипл. строка, если ничего не будет найдено
        ДОБАВИТЬ_В_БД(ПАРАМЕТРЫ)
            (True) - операция прошла успешно
            (False) - данные значения уже существуют в таблице
        УДАЛИТЬ_В_БД(ПАРАМЕТРЫ)
            (True) - операция прошла успешно
            (False) - данные значения уже отсутствуют в таблице
        (None) - результат условия ложный
    Вызывает исключение ValueError, если:
        неверный формат правила
        неверный формат действия в правиле
        искомый триплет не найден в триплексной строке
     Вызывает исключение FileNotFoundError, если:
        по принятому пути не существует БД SQLite
    """
    # класс для удобного доступа к данным контекста из кода и внесения изменений
    class Context:
        def __init__(self, data):
            # db - database
            # mdb - metadatabase - база метаданных
            self.trp_str = parse_trp_str(data['trp_str'])
            self.trp_str_from_db = parse_trp_str(data['trp_str_from_db'])
            self.path_db = data['path_db']
            self.type_db = data['type_db']
            self.path_mdb = data['path_mdb']
            self.type_mdb = data['type_mdb']
            self.agent = data['agent']
            self.engine_db = _create_engine(self.path_db, self.type_db)

    # context
    ctx = Context(ctx)

    # парсинг правила
    parsed_rule = re.findall(RE_RULE, rule)
    if len(parsed_rule) == 0:
        raise ValueError('Неверный формат правила: ' + rule)
    cond, action = parsed_rule[0][0], parsed_rule[0][1]

    # проверка истинности условия
    cond_result = check_condition(cond, ctx.trp_str, ctx.trp_str_from_db)
    if not cond_result:
        return None

    # ====================SELECT====================
    if re.match(RE_ACT_FIND_IN_DB, action) is not None:
        order_value = ''  # часть sql-запроса для сортировки
        # если в запросе есть указание сортировки
        if re.match(RE_ACT_FIND_IN_DB_WO, action):
            trp_cond, order_by, type_of_order = re.findall(RE_ACT_FIND_IN_DB_WO, action)[0]
            order_by = order_by.split('.')
            order_by = _get_agent_tbl_cln(order_by[0], order_by[1], ctx)
            type_of_order = 'DESC' if type_of_order == '-' else 'ASC'
            order_value = ' ORDER BY ' + order_by + ' ' + type_of_order
        else:
            trp_cond = re.findall(RE_ACT_FIND_IN_DB, action)[0]

        # формирование sql-запроса
        # WARN небезопасно; как переписать?
        nm_of_agent_tbl = _determine_table_of_agent(ctx.path_mdb, ctx.type_mdb, ctx.agent)
        trp_cond = _rewrite_cond(trp_cond, ctx)
        # имя колонки с назв. инструментов для данн. агента
        # WARN решается неверная задача - исправить
        # _ = tuple()
        nm_of_instr_cln = _get_agent_tbl_cln(_tmp_prefix, _tmp_name, ctx)
        sql_query = 'SELECT ' + nm_of_instr_cln + ' ' \
                    'FROM ' + nm_of_agent_tbl + ' ' \
                    'WHERE ' + trp_cond
        sql_query += order_value

        query_result = ctx.engine_db.execute(sql_query).fetchall()

        # формирование трипл. строки по результатам запроса
        # к новым триплетам с повторяющимся префиксом к последнему прибавляется порядковый номер: E, E1, E2 и т.д.
        result = TrpStr(*(
            Trp(_tmp_prefix if i == 0 else _tmp_prefix + str(i), _tmp_name, instr[0])
            for i, instr in enumerate(query_result)
        ))

        return result

    # ====================INSERT====================
    elif re.match(RE_ACT_ADD_IN_DB, action) is not None:
        prefix = re.findall(RE_ACT_ADD_IN_DB, action)[0]
        rslt, prms_for_query, tbl, where_vals = _make_context_for_action(prefix, ctx)

        # в таблице уже есть значения по запросу
        if rslt > 0:
            return False

        sql_query = tbl.insert().values(**prms_for_query)  # формирование sql-запроса
        ctx.engine_db.execute(sql_query)
        return True

    # ====================DELETE====================
    elif re.match(RE_ACT_DEL_FROM_DB, action) is not None:
        prefix = re.findall(RE_ACT_DEL_FROM_DB, action)[0]
        rslt, prms_for_query, tbl, where_vals = _make_context_for_action(prefix, ctx)

        # в таблице итак отстутствуют значения по запросу
        if rslt == 0:
            return False

        sql_query = tbl.delete().where(and_(*where_vals))  # формирование sql-запроса
        ctx.engine_db.execute(sql_query)
        return True

    # ==========Неверный формат действия===========
    else:
        raise ValueError('Неверный формат действия: ' + action)


def _rewrite_cond(str_to_rpl, ctx):
    """
    ПРИВЕСТИ УСЛОВИЕ ДЛЯ ЗАПРОСА К SQL-ФОРМАТУ
    Триплеты в параметрах действия правила без "$", заменяются соответствующими значениями из онтологического словаря
    согласно заданному агенту
    Принимает:
        str_to_rpl (str) - условие для запроса
        ctx (Context) - метаданные
    Возвращает:
        (str) - изменённое условие
    """
    #  копипаст из check_condition
    # замена операторов
    replacements = [[' или ', ' OR '],
                    [' и ',   ' AND '],
                    [' ИЛИ ', ' OR '],
                    [' И ',   ' AND ']]
    for rplc in replacements:
        str_to_rpl = str_to_rpl.replace(rplc[0], rplc[1])

    # поиск триплетов в трипл. строке
    for trp in re.findall(RE_PREFIX_NAME_NI, str_to_rpl):  # замена триплетов на их значения
        value = ctx.trp_str[trp[1:]]  # получаем значение триплета
        if value is None:
            raise ValueError('Триплет {} не найден в триплексной строке'.format(trp))
        value = "'{}'".format(value) if isinstance(value, str) else str(value)  # приведение значений триплета к формату
        str_to_rpl = str_to_rpl.replace(trp, value)

    # поиск значений триплетов для SQL-запроса в базе согласно заданному агенту
    for trp in re.findall(RE_PREFIX_NAME_WODS_NI, str_to_rpl):
        prefix, name = trp.split('.')
        cln = _get_agent_tbl_cln(prefix, name, ctx)
        str_to_rpl = str_to_rpl.replace(trp, cln)

    return str_to_rpl


def _make_context_for_action(prefix, ctx):
    """
    СОЗДАНИЕ КОНТЕКСТА ДЛЯ ВЫПОЛНЕНИЕ SQL-ЗАПРОСА
    В качестве параметра передаётся префикс, согласно которому
    необходимо выбрать все триплеты из переданной трипл. строки
    """
    # {столбец таблицы: значение, ...}
    def _(trp):
        return _get_agent_tbl_cln(trp.prefix, trp.name, ctx)
    prms_for_query = {_(trp): trp.value for trp in ctx.trp_str[prefix]}

    # формирование sql-запроса на проверку наличия инструмента в базе
    nm_of_agent_tbl = _determine_table_of_agent(ctx.path_mdb, ctx.type_mdb, ctx.agent)
    columns = tuple(Column(col) for col in prms_for_query)
    tbl = Table(nm_of_agent_tbl, MetaData(), *columns)
    where_vals = tuple(getattr(tbl.c, col) == prms_for_query[col] for col in prms_for_query)
    sql_query = select([func.count(tbl)]).where(and_(*where_vals))

    rslt = ctx.engine_db.execute(sql_query).scalar()
    return rslt, prms_for_query, tbl, where_vals


@lru_cache(maxsize=4)  # кэширование запросов
def _determine_table_of_agent(path_mdb, type_mdb, agent):
    """ОПРЕДЕЛЕНИЕ НАЗВАНИЯ ТАБЛИЦЫ ПО ИМЕНИ АГЕНТА"""
    tbl = Table(_tmp_agents_tbl, MetaData(), Column(_tmp_agents_tbl_c_name), Column(_tmp_agents_tbl_c_db))
    sql_query = select([getattr(tbl.c, _tmp_agents_tbl_c_db)]).where(getattr(tbl.c, _tmp_agents_tbl_c_name) == agent)
    engine = _create_engine(path_mdb, type_mdb)
    nm_of_agent_tbl = engine.execute(sql_query).scalar()
    return nm_of_agent_tbl


@lru_cache(maxsize=8)  # кэширование запросов
def _create_engine(path_db, type_db):
    """Создание объекта-движка для определённой БД"""
    # TODO возможны различные тонкости при подключении к базам данных
    # TODO указание драйвера
    db_mark = ''
    if type_db == 'SQLite' or type_db == 1:
        if not exists_file(path_db):
            raise FileNotFoundError('По принятому пути не существует БД SQLite: ' + str(path_db))
        db_mark = r'sqlite:///'
    elif type_db == 'MS SQL Server' or type_db == 2:
        pass
    else:
        raise ValueError('Неподдерживаемая СУБД: ' + str(type_db))
    return create_engine(db_mark + path_db, echo=False)
