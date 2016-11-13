# -*- coding: utf-8 -*-
""" Реализация правил в ВСПТД """

# WARN TEMP в настоящий момент работа осуществляется только с SQLite
# WARN в некоторых местах не используется экранирование, т.о. потенциально небезопасно

import re
from sqlalchemy import create_engine, Table, MetaData, Column
from functools import lru_cache
from os.path import isfile as exists_file

from vsptd import Trp, TrpStr, parse_trp_str, check_condition, RE_PREFIX_NAME_NI, RE_PREFIX_NAME_WODS_NI
from getVoc import trpGetOntVocSQL

# TEMP
agent_tbl_nm = 'AGENTS'  # название таблицы в базе метаданных с данными таблиц агентов
# префикс и имя для выборки инструментов из базы
_prefix = 'E'
_name = 'NM'

RE_RULE = re.compile('^ЕСЛИ (.+) ТО (.+);$')  # правило
RE_ACT_FIND_IN_DB = re.compile(r'^НАЙТИ_В_БД\((.*)\)$')  # искать в БД
RE_ACT_FIND_IN_DB_WO = re.compile(r'^НАЙТИ_В_БД\((.*)\\\\(.*)(\+|-)\\\\\)$')  # искать в БД (with order by)
RE_ACT_ADD_IN_DB = re.compile(r'^ДОБАВИТЬ_В_БД\(([A-Za-z]+\d*)\)$')  # добавить в БД
RE_ACT_DEL_FROM_DB = re.compile(r'^УДАЛИТЬ_В_БД\(([A-Za-z]+\d*)\)$')  # удалить из БД


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
            trp_str (str) - триплексная строка
            trp_str_from_db (str) - триплексная строка по данным из базы данных
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

    # context
    ctx = Context(ctx)

    # парсинг правила
    parsed_rule = re.findall(RE_RULE, rule)
    if len(parsed_rule) == 0:
        raise ValueError('Неверный формат правила: ' + rule)
    cond, action = parsed_rule[0][0], parsed_rule[0][1]

    # проверка истинности условия
    cond_result = check_condition(ctx.trp_str, cond, ctx.trp_str_from_db)
    if not cond_result:
        return None

    # ====================SELECT====================
    if re.match(RE_ACT_FIND_IN_DB, action) is not None:
        is_order = False  # флан наличия сортировки результата
        # если в запросе есть указание сортировки
        if re.match(RE_ACT_FIND_IN_DB_WO, action):
            is_order = True
            trp_cond, order_by, type_of_order = re.findall(RE_ACT_FIND_IN_DB_WO, action)[0]
            order_by = _replace_val_of_trps(order_by, ctx)
            type_of_order = 'DESC' if type_of_order == '-' else 'ASC'
        else:
            trp_cond = re.findall(RE_ACT_FIND_IN_DB, action)[0]

        # формирование sql-запроса
        nm_of_agent_tbl = _determine_table_of_agent(ctx)
        trp_cond = _replace_val_of_trps(trp_cond, ctx)
        # имя колонки с назв. инструментов для данн. агента
        nm_of_instr_cln = trpGetOntVocSQL(_prefix, _name, ctx.path_mdb, ctx.type_mdb)[(ctx.agent, 'NAME')]
        sql_query = 'SELECT ' + nm_of_instr_cln + ' ' \
                    'FROM ' + nm_of_agent_tbl + ' ' \
                    'WHERE ' + trp_cond
        if is_order:
            sql_query += ' ORDER BY ' + order_by + ' ' + type_of_order

        engine = _create_engine(ctx.path_db, ctx.type_db)
        query_result = engine.execute(sql_query).fetchall()

        # формирование трипл. строки по результатам запроса
        # к новым триплетам с повторяющимся префиксом к последнему прибавляется порядковый номер: E, E1, E2 и т.д.
        result = TrpStr(*(
            Trp(_prefix if i == 0 else _prefix + str(i), _name, instr[0])
            for i, instr in enumerate(query_result)
        ))

        return result

    # ====================INSERT====================
    elif re.match(RE_ACT_ADD_IN_DB, action) is not None:
        prefix = re.findall(RE_ACT_ADD_IN_DB, action)[0]

        rslt, prms_for_query, nm_of_agent_tbl, WHERE_vals = _make_context_for_action(prefix, ctx)

        # в таблице уже есть значения
        if rslt > 0:
            return False

        # формирование sql-запроса
        # colons = prms_for_query.keys()
        # VALUES_vals = tuple(prms_for_query[colon] for colon in colons)
        columns = tuple(Column(col) for col in prms_for_query)
        tbl = Table(nm_of_agent_tbl, MetaData(), *columns)
        sql_query = tbl.insert().values(**prms_for_query)
        # sql_query = 'INSERT INTO ' + nm_of_agent_tbl + ' ' + \
        #             str(tuple(colons)) + ' ' \
        #             'VALUES ' + str(VALUES_vals)

        engine = _create_engine(ctx.path_db, ctx.type_db)
        engine.execute(sql_query)

        return True

    # ====================DELETE====================
    elif re.match(RE_ACT_DEL_FROM_DB, action) is not None:
        prefix = re.findall(RE_ACT_DEL_FROM_DB, action)[0]

        rslt, prms_for_query, nm_of_agent_tbl, WHERE_vals = _make_context_for_action(prefix, ctx)

        # в таблице отстутствуют значения
        if rslt == 0:
            return False

        # формирование sql-запроса
        sql_query = 'DELETE FROM ' + nm_of_agent_tbl + ' ' + \
                    'WHERE ' + WHERE_vals
        engine = _create_engine(ctx.path_db, ctx.type_db)
        engine.execute(sql_query)

        return True

    # ==========Неверный формат действия===========
    else:
        raise ValueError('Неверный формат действия: ' + action)


def _replace_val_of_trps(str_to_rpl, ctx):
    """
    ЗАМЕНИТЬ ТРИПЛЕТЫ В ПРИНИМАЕМОЙ СТРОКЕ НА ИХ ЗНАЧЕНИЯ
    Триплеты в параметрах действия правила без "$", заменяются соответствующими значениями из онтологического словаря
    согласно заданному агенту
    Принимает:
        trp_cond - условие для запроса
        trp_str (str) - триплексная строка
        agent (str) - имя агента
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
        result = trpGetOntVocSQL(prefix, name, ctx.path_mdb, ctx.type_mdb)[(ctx.agent, 'NAME')]
        str_to_rpl = str_to_rpl.replace(trp, result)

    return str_to_rpl


def _make_context_for_action(prefix, ctx):
    """
    СОЗДАНИЕ КОНТЕКСТА ДЛЯ ВЫПОЛНЕНИЕ SQL-ЗАПРОСА
    В качестве параметра передаётся префикс, согласно которому
    необходимо выбрать все триплеты из переданной трипл. строки
    """
    # TODO CHECK оптимизировать
    prms_for_query = {trpGetOntVocSQL(trp.prefix, trp.name, ctx.path_mdb, ctx.type_mdb)[(ctx.agent, 'NAME')]: trp.value
                      for trp in ctx.trp_str[prefix]
                      }
    nm_of_agent_tbl = _determine_table_of_agent(ctx)

    # формирование sql-запроса на проверку наличия инструмента в базе
    # формирование значений для конструкции WHERE
    WHERE_vals = ''
    for i, key in enumerate(prms_for_query):
        WHERE_vals += key + '='
        WHERE_vals += "'{}'".format(prms_for_query[key]) \
            if isinstance(prms_for_query[key], str) \
            else str(prms_for_query[key])
        if i < len(prms_for_query) - 1:
            WHERE_vals += ' AND '
    sql_query = 'SELECT COUNT (*) ' \
                'FROM ' + nm_of_agent_tbl + ' ' \
                'WHERE ' + WHERE_vals

    engine = _create_engine(ctx.path_db, ctx.type_db)
    rslt = engine.execute(sql_query).scalar()

    return rslt, prms_for_query, nm_of_agent_tbl, WHERE_vals


@lru_cache(maxsize=8)  # кэширование запросов
def _determine_table_of_agent(ctx):
    """ОПРЕДЕЛЕНИЕ ПО ИМЕНИ АГЕНТА НАЗВАНИЯ СООТВЕТСТВУЮЩЕЙ ТАБЛИЦЫ"""
    engine = _create_engine(ctx.path_mdb, ctx.type_mdb)
    sql_query = 'SELECT DB FROM ' + agent_tbl_nm + ' WHERE NAME=:agent'  # TEMP используется DB для названия таблицы
    nm_of_agent_tbl = engine.execute(sql_query, agent=ctx.agent).scalar()
    return nm_of_agent_tbl


def _create_engine(path_db, type_db):
    """Создание объекта-движка для определённой БД"""
    # TODO возможны различные тонкости при подключении к базам данных
    # TODO указание драйвера
    # TODO проверка существования базы
    db_mark = ''
    if type_db == 'SQLite' or type_db == 1:
        if not exists_file(path_db):
            raise ValueError('По принятому пути не существует БД SQLite: ' + str(path_db))
        db_mark = r'sqlite:///'
    elif type_db == 'MS SQL Server' or type_db == 2:
        pass
    elif type_db == 'MongoDB' or type_db == 3:
        pass
    else:
        raise ValueError('Неподдерживаемая СУБД: ' + str(type_db))
    return create_engine(db_mark + path_db)
