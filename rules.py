# -*- coding: utf-8 -*-
""" Реализация правил в ВСПТД """
import re
from sqlalchemy import create_engine

from vsptd import Trp, TrpStr, parse_trp_str, check_condition, RE_PREFIX_NAME_NI, RE_PREFIX_NAME_WODS_NI
from getVoc import trpGetOntVoc

# TEMP может быть и не sqlite
# WARN в некоторых местах не используется экранирование, т.к. оно почему-то
# WARN не хочет нормально работать, т.о. потенциально небезопасно

# TEMP
dict_of_agents = 'agents.sqlite'  # путь к словарю агентов

RE_RULE = re.compile('^ЕСЛИ (.+) ТО (.+);$')  # правило
RE_ACT_FIND_IN_DB = re.compile(r'^НАЙТИ_В_БД\((.*)\\\\(.*)(\+|-)\\\\\)$')  # искать в БД
RE_ACT_ADD_IN_DB = re.compile(r'^ДОБАВИТЬ_В_БД\(([A-Za-z]+\d*)\)$')  # добавить в БД
RE_ACT_DEL_FROM_DB = re.compile(r'^УДАЛИТЬ_В_БД\(([A-Za-z]+\d*)\)$')  # удалить из БД


def exec_rule(rule, trp_str, path_base, agent, trp_str_from_db=''):
    """
    РАСЧЁТ ПРАВИЛА
    Виды правил:
        - ЕСЛИ УСЛОВИЕ ТО НАЙТИ_В_БД(ПАРАМЕТРЫ);
        - ЕСЛИ УСЛОВИЕ ТО ДОБАВИТЬ_В_БД(ПАРАМЕТРЫ);
        - ЕСЛИ УСЛОВИЕ ТО УДАЛИТЬ_В_БД(ПАРАМЕТРЫ);
    Триплеты в условии правила без "$", заменяются соответствующими значениями, указанными в trp_str_from_db
    Триплеты в параметрах действия правила без "$", заменяются соответствующими значениями из онтологического словаря
    согласно заданному агенту
    Принимает:
        rule (str) - правило. Например: ЕСЛИ $L.WOB=25 ТО ДОБАВИТЬ_В_БД('E');
        trp_str (str) - триплексная строка
        path_base (str) - путь к базе, к которой будет осуществлён SQL-запрос
        agent (str) - имя агента
        trp_str_from_db (str) необяз. - триплексная строка по данным из базы данных
    Возвращает:
        НАЙТИ_В_БД(ПАРАМЕТРЫ);
            (TrpString) - результаты поискового запроса. Вернётся пустая трипл. строка, если ничего не будет найдено
        ДОБАВИТЬ_В_БД(ПАРАМЕТРЫ);
            (True) - операция прошла успешно
            (False) - данные значения уже существуют в таблице
        УДАЛИТЬ_В_БД(ПАРАМЕТРЫ);
            (True) - операция прошла успешно
            (False) - данные значения уже отсутствуют в таблице
        (None) - результат условия ложный
    Вызывает исключение ValueError, если:
        неверный формат правила
        неверный формат действия в правиле
        искомый триплет не найден в триплексной строке
    """
    # парсинг правила
    parsed_rule = re.findall(RE_RULE, rule)
    if len(parsed_rule) == 0:
        raise ValueError('Неверный формат правила: ' + rule)
    cond, action = parsed_rule[0][0], parsed_rule[0][1]

    # проверка истинности условия
    cond_result = check_condition(trp_str, cond, trp_str_from_db)
    if not cond_result:
        return None

    # определение типа правила
    if re.match(RE_ACT_FIND_IN_DB, action) is not None:
        command = 'SELECT'
    elif re.match(RE_ACT_ADD_IN_DB, action) is not None:
        command = 'INSERT'
    elif re.match(RE_ACT_DEL_FROM_DB, action) is not None:
        command = 'DELETE'
    else:
        raise ValueError('Неверный формат действия: ' + action)

    if command == 'SELECT':
        # формирование sql-запроса
        trp_cond, order_by, type_of_order = re.findall(RE_ACT_FIND_IN_DB, action)[0]
        nm_of_agent_tbl = _determine_table_of_agent(agent)
        trp_cond = _replace_val_of_trps(trp_cond, trp_str, agent)
        order_by = _replace_val_of_trps(order_by, trp_str, agent)
        type_of_order = 'DESC' if type_of_order == '-' else 'ASC'
        nm_of_instr_cln = trpGetOntVoc('E', 'NM')[agent + '.NAME']  # имя колонки с назв. инструментов для данн. агента
        sql_query = 'SELECT ' + nm_of_instr_cln + ' ' \
                    'FROM ' + nm_of_agent_tbl + ' ' \
                    'WHERE ' + trp_cond + ' ' \
                    'ORDER BY ' + order_by + ' ' + type_of_order

        engine = create_engine(r'sqlite:///' + path_base)
        query_result = engine.execute(sql_query).fetchall()

        # формирование трипл. строки по результатам запроса
        result = TrpStr(*(
            Trp('E' if i == 0 else 'E' + str(i), 'NM', instr[0])
            for i, instr in enumerate(query_result)
        ))

        return result

    elif command == 'INSERT':
        prefix = re.findall(RE_ACT_ADD_IN_DB, action)[0]

        rslt, prms_for_query, nm_of_agent_tbl, WHERE_vals = _make_context_for_action(trp_str, path_base, agent, prefix)

        # в таблице уже есть значения
        if rslt > 0:
            return False

        # формирование sql-запроса
        nm_of_agent_tbl = _determine_table_of_agent(agent)
        colons = prms_for_query.keys()
        VALUES_vals = tuple(prms_for_query[colon] for colon in colons)
        sql_query = 'INSERT INTO ' + nm_of_agent_tbl + ' ' + \
                    str(tuple(colons)) + ' ' \
                    'VALUES ' + str(VALUES_vals)

        engine = create_engine(r'sqlite:///' + path_base)
        engine.execute(sql_query)

        return True

    elif command == 'DELETE':
        prefix = re.findall(RE_ACT_DEL_FROM_DB, action)[0]

        rslt, prms_for_query, nm_of_agent_tbl, WHERE_vals = _make_context_for_action(trp_str, path_base, agent, prefix)

        # в таблице отстутствуют значения
        if rslt == 0:
            return False

        # формирование sql-запроса
        nm_of_agent_tbl = _determine_table_of_agent(agent)
        sql_query = 'DELETE FROM ' + nm_of_agent_tbl + ' ' + \
                    'WHERE ' + WHERE_vals
        engine = create_engine(r'sqlite:///' + path_base)
        engine.execute(sql_query)

        return True


def _replace_val_of_trps(str_to_rpl, trp_str, agent):
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
    trp_str = parse_trp_str(trp_str)

    # WARN копипаст из check_condition
    # замена операторов
    replacements = [[' или ', ' or '],
                    [' и ', ' and '],
                    [' ИЛИ ', ' or '],
                    [' И ', ' and ']]
    for rplc in replacements:
        str_to_rpl = str_to_rpl.replace(rplc[0], rplc[1])

    # поиск триплетов в трипл. строке
    for trp in re.findall(RE_PREFIX_NAME_NI, str_to_rpl):  # замена триплетов на их значения
        value = trp_str[trp[1:]]  # получаем значение триплета
        if value is None:
            raise ValueError('Триплет {} не найден в триплексной строке'.format(trp))
        value = "'{}'".format(value) if isinstance(value, str) else str(value)  # приведение значений триплета к формату
        str_to_rpl = str_to_rpl.replace(trp, value)

    # поиск значений триплетов для SQL-запроса в базе согласно заданному агенту
    for trp in re.findall(RE_PREFIX_NAME_WODS_NI, str_to_rpl):
        result = trpGetOntVoc(*trp.split('.'))[agent + '.NAME']
        str_to_rpl = str_to_rpl.replace(trp, result)

    return str_to_rpl


def _make_context_for_action(trp_str, path_base, agent, prefix):
    """СОЗДАНИЕ КОНТЕКСТА ДЛЯ ВЫПОЛНЕНИЕ SQL-ЗАПРОСА"""
    # в качестве параметра передаётся префикс, согласно которому
    # необходимо выбрать все триплеты из переданной трипл. строки
    prms_for_query = {trpGetOntVoc(trp.prefix, trp.name)[agent + '.NAME']: trp.value
                      for trp in parse_trp_str(trp_str)[prefix]
                      }
    nm_of_agent_tbl = _determine_table_of_agent(agent)

    # формирование sql-запроса на проверку наличия инструмента в базе
    # формирование значений для конструкции WHERE
    WHERE_vals = ''
    for i, k in enumerate(prms_for_query):
        WHERE_vals += k + '='
        WHERE_vals += "'{}'".format(prms_for_query[k]) \
            if isinstance(prms_for_query[k], str) \
            else str(prms_for_query[k])
        if i < len(prms_for_query) - 1:
            WHERE_vals += ' AND '
    sql_query = 'SELECT COUNT (*) ' \
                'FROM ' + nm_of_agent_tbl + ' ' \
                'WHERE ' + WHERE_vals

    engine = create_engine(r'sqlite:///' + path_base)
    rslt = engine.execute(sql_query).scalar()

    return rslt, prms_for_query, nm_of_agent_tbl, WHERE_vals


def _determine_table_of_agent(agent):
    """ОПРЕДЕЛЕНИЕ ПО ИМЕНИ АГЕНТА НАЗВАНИЯ СООТВЕТСТВУЮЩЕЙ ТАБЛИЦЫ"""
    engine = create_engine(r'sqlite:///' + dict_of_agents)
    sql_query = 'SELECT DB FROM AGENTS WHERE NAME=:agent'
    nm_of_agent_tbl = engine.execute(sql_query, agent=agent).scalar()
    return nm_of_agent_tbl
