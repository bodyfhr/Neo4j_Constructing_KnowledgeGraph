#-*- coding: UTF-8 -*-
# @Time    : 2020-12-28 10:20:32
# @Author  : 费海瑞
# @File    : Disease_Data.py
# @Software: PyCharm


from py2neo import Graph,Node
import csv

######################连接neo4j数据库
graph = Graph(
    "http://localhost:7474",
    username="neo4j",
    password="123456"
    )

#清除所有节点
def delete():
    node_list=['cn疾病','检查','症状','科室','药品','食物','疾病同义词','症状同义词','症状属性']
    for i in node_list:
        graph.run("MATCH (n:%s)-[r]-() DELETE n,r" % (i))
    for i in node_list:
        graph.run("MATCH (n:%s) delete n" % (i))
delete()


####################疾病相关数据录取
def disease():
    s=0
    csv_file=csv.reader(open('data/Disease_Data.csv','r',encoding='utf-8'))
    for lin in csv_file:
        s+=1

        if lin[0] in ['疾病名称','']:
            continue
        print(s, lin)
        #疾病名称,疾病定义,疾病病因,疾病预防,治疗费用,治疗方案,治疗周期,医保疾病,所属科室,宜吃食物,忌吃食物,推荐食谱,推荐药品,所需检查,易感人群,疾病症状,疾病并发症,传染方式,治愈率,患病率,疾病护理,疾病类别
        Disease=Node('cn疾病',疾病名称=lin[0],疾病定义=lin[1],疾病病因=lin[2],疾病预防=lin[3],治疗费用=lin[4],治疗方案=lin[5],治疗周期=lin[6],
                     医保疾病=lin[7],所属科室=lin[8],宜吃食物=lin[9],忌吃食物=lin[10],推荐食谱=lin[11],推荐药品=lin[12],所需检查=lin[13],易感人群=lin[14],
                     疾病症状=lin[15],疾病并发症=lin[16],传染方式=lin[17],治愈率=lin[18],患病率=lin[19],疾病护理=lin[20],严重程度=lin[21],疾病类别=lin[22],常不常见=lin[23],疾病同义词=lin[24])
        graph.create(Disease)
        la=graph.run("MATCH (n:cn疾病) WITH  n.疾病名称 AS id, COLLECT(n) AS nodelist, COUNT(*) AS count  WHERE count >1  CALL apoc.refactor.mergeNodes(nodelist)  YIELD  node  RETURN  node")

        for i in lin[8].split(","):
            if i != '':
                ks = Node('科室',科室=lin[8])

                graph.create(ks)
                graph.run('MATCH (n:科室) WITH n.科室 AS id, COLLECT(n) AS nodelist, COUNT(*) AS count  WHERE count > 1 '
                  'CALL apoc.refactor.mergeNodes(nodelist) YIELD node RETURN node')

                # disease_ks = Relationship(Disease, '所属科室', ks)
                graph.run("match (n:科室{科室:'%s'}),(m:cn疾病{疾病名称:'%s'}) merge (n)<-[r:所需科室]-(m) return n,m,r;" % (i,lin[0]))
                # graph.create(disease_ks)

        for i in lin[11].split(","):
            if i != '':
                sw = Node('食物', 食物=i)
                graph.create(sw)

                graph.run('MATCH (n:食物) WITH n.食物 AS id, COLLECT(n) AS nodelist, COUNT(*) AS count  WHERE count > 1 '
                          'CALL apoc.refactor.mergeNodes(nodelist) YIELD node RETURN node')

                # disease_yp = Relationship(Disease, '推荐食谱', yp)
                graph.run(
                    "match (n:食物{食物:'%s'}),(m:cn疾病{疾病名称:'%s'}) merge (n)<-[r:推荐食谱]-(m) return n,m,r;" % (i, lin[0]))
                # graph.create(disease_yp)

        for i in lin[12].split(","):
            if i !='':
                yp = Node('药品', 药品=i)
                graph.create(yp)

                graph.run('MATCH (n:药品) WITH n.药品 AS id, COLLECT(n) AS nodelist, COUNT(*) AS count  WHERE count > 1 '
                          'CALL apoc.refactor.mergeNodes(nodelist) YIELD node RETURN node')

                # disease_yp = Relationship(Disease, '推荐药品', yp)
                graph.run("match (n:药品{药品:'%s'}),(m:cn疾病{疾病名称:'%s'}) merge (n)<-[r:推荐药品]-(m) return n,m,r;" % (i,lin[0]))
                # graph.create(disease_yp)

        for i in lin[13].split(","):
            if i != '':
                yp = Node('检查', 检查=i)
                graph.create(yp)
                graph.run('MATCH (n:检查) WITH n.检查 AS id, COLLECT(n) AS nodelist, COUNT(*) AS count  WHERE count > 1 '
                          'CALL apoc.refactor.mergeNodes(nodelist) YIELD node RETURN node')

                # disease_yp = Relationship(Disease, '所需检查', yp)
                graph.run("match (n:检查{检查:'%s'}),(m:cn疾病{疾病名称:'%s'}) merge (n)<-[r:所需检查]-(m) return n,m,r;" % (i,lin[0]))

                # graph.create(disease_yp)
        for i in lin[15].split(","):
            if i != '':
                yp = Node('症状', 症状=i)
                graph.create(yp)
                graph.run('MATCH (n:症状) WITH n.症状 AS id, COLLECT(n) AS nodelist, COUNT(*) AS count  WHERE count > 1 '
                          'CALL apoc.refactor.mergeNodes(nodelist) YIELD node RETURN node')
                # disease_yp = Relationship(Disease, '疾病症状', yp)
                graph.run("match (n:症状{症状:'%s'}),(m:cn疾病{疾病名称:'%s'}) merge (n)<-[r:疾病症状]-(m) return n,m,r;" % (i,lin[0]))
                # graph.create(disease_yp)

        for i in lin[24].split(","):
            if i != '':
                yp = Node('疾病同义词', 疾病同义词=i)
                graph.create(yp)
                graph.run('MATCH (n:疾病同义词) WITH n.疾病同义词 AS id, COLLECT(n) AS nodelist, COUNT(*) AS count  WHERE count > 1 '
                          'CALL apoc.refactor.mergeNodes(nodelist) YIELD node RETURN node')
                # disease_yp = Relationship(Disease, '疾病同义词', yp)
                graph.run(
                    "match (n:疾病同义词{疾病同义词:'%s'}),(m:cn疾病{疾病名称:'%s'}) merge (n)<-[r:疾病同义词]-(m) return n,m,r;" % (i, lin[0]))
                # graph.create(disease_yp)

    return '完成'

disease()



