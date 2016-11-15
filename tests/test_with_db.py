# -*- coding: utf-8 -*-
# поиск модулей и на уровень выше
import sys
sys.path.append('..\\')

from dmo import exec_rule


# контекст
ctx = {
    'trp_str':
        "$L.D=35;$L.L=10;$L.SE='221440';$L.KW=12;$E.L=36;$E.D=13;$O.GRO='20001';$E.NM='Отвёртка';$L.WOB=27;$M.PGM=3;",
    'trp_str_from_db': '',
    'path_db': r'bases/base.sqlite',
    'type_db': 'SQLite',
    'path_mdb': r'bases/metabase.sqlite',
    'type_mdb': 'SQLite',
    'agent': 'VERT'
}


# НАЙТИ В БД
rule = r'ЕСЛИ $L.D=35 ТО НАЙТИ_В_БД(E.D<$L.D И E.L>$L.L);'
result = exec_rule(rule, ctx)
print(rule, result, sep='\n', end='\n\n')


# НАЙТИ В БД (С СОРТИРОВКОЙ)
rule = r'ЕСЛИ $L.D=35 ТО НАЙТИ_В_БД(E.D<$L.D И E.L>$L.L\\E.D-\\);'
result = exec_rule(rule, ctx)
print(rule, result, sep='\n', end='\n\n')


# ДОБАВИТЬ В БД
rule = r'ЕСЛИ $L.L=10 И $L.KW=12 ТО ДОБАВИТЬ_В_БД(E);'
result = exec_rule(rule, ctx)
print(rule, result, sep='\n', end='\n\n')


# УДАЛИТЬ В БД
rule = r'ЕСЛИ 1 ТО УДАЛИТЬ_В_БД(E);'
result = exec_rule(rule, ctx)
print(rule, result, sep='\n')
