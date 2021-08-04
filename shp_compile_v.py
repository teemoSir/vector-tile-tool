#!/usr/bin/python
# coding=utf-8
import threading
import subprocess
from osgeo import gdal
import os
import sys
from osgeo import ogr
import time
import json

# 排除文件名称
source_shp_name_filter = ["Node", "Bnd", "POI", "City", "Country", "County", "Province"]

# 增加需处理文件
# source_shp_type_filter = ["POI.dbf"]

# 需切图数据
source_json_filter = [
    "Road",
    "LandUseArea",
    "LandUseLine",
    "SettlementArea",
    "SettlementLine",
    "Railway",
    "poi_cms"
]

# 需要切图数据对应缩写
json_name = {
    "Road": "e",
    "LandUseArea": "f",
    "LandUseLine": "g",
    "SettlementArea": "h",
    "SettlementLine": "l",
    "Railway": "j",
    "poi_cms": "k"
}

# 属性需坐标转换字段
source_shp_properties_filer = ["XMIN", "YMIN", "XMAX", "YMAX", "X_COORD", "Y_COORD", "X_ENTR", "Y_ENTR"]

# 属性需清除poi乱码^与道路 None
source_shp_properties_filer_garbled = [
    "NAME_CHN", "NAME_TRD", "NAME_PY", "NAME_ENG", "ALIAS_CHN", "ALIAS_TRD",
    "ALIAS_PY", "ALIAS_ENG", "ROUTE_NO"
]

# 源数据位置
file_path = "D:/vector-tile-program/16Q38M_LD_Src_Data_MESH"
out_path = "D:/vector-tile-program/16Q38M_LD_Src_Data_MESH_OUT"

# 全局状态
job_json = {}

# 切图规则
tipp_rules = {
    "LandUseArea":
        [
            '''{vt} -e {0}/china_landusearea -l china_landusearea -F -pC -Z8 -z14 -g 1'''
        ],
    "LandUseLine":
        [
            '''{vt} -e {0}/china_landuseline -l china_landuseline -F -pC -Z8 -z14 -g 1'''
        ],
    "Road":
        [
            '''{vt} -e {0}/china_way -l china_way -F -pC -j '{"*":["all",["in","FC",1,2]]}' -Z8 -z9 -g 0.6''',
            '''{vt} -e {0}/china_way -l china_way -F -pC -j '{"*":["all",["in","FC",1,2,3]]}' -Z10 -z11 -g 0.8''',
            '''{vt} -e {0}/china_way -l china_way -F -pC -j '{"*":["all",["in","FC",1,2,3,4]]}' -Z12 -z13 -g 1''',
            '''{vt} -e {0}/china_way -l china_way -F -pC -Z14 -z14'''
        ],
    "SettlementArea":
        [
            '''{vt} -e {0}/china_settlementarea -l china_settlementarea -F -pC -Z14 -z14'''
        ],
    "SettlementLine":
        [
            '''{vt} -e {0}/china_settlementline -l china_settlementline -F -pC -Z14 -z14'''
        ],
    "Railway":
        [
            '''{vt} -e {0}/china_railway -l china_railway -F -pC -Z6 -z14 -as '--simplification=10' -pk'''
        ],
    "poi_cms":
        [
            '''{vt} -e {0}/china_poi_10 -l china_poi -j '{ "*": ["all",[ "in", "Type","250102","290200","290201","290202"]] }' -F -pC -Z10 -z10''',
            '''{vt} -e {0}/china_poi_11 -l china_poi -j '{ "*": ["all",[ "in", "Type","210201","210202","241201","250200"]] }' -F -pC -Z11 -z11''',
            '''{vt} -e {0}/china_poi_12 -l china_poi -j '{ "*": ["all",[ "in", "Type","180101","210203","210103","210102","210104","290106"]] }' -F -pC -Z12 -z12''',
            '''{vt} -e {0}/china_poi_13 -l china_poi -j '{ "*": ["all",[ "in", "Type","190101","220201","250301","250302","250303","250300","250400","290203"]] }' -F -pC -Z13 -z13''',
            '''{vt} -e {0}/china_poi_14 -l china_poi -j '{ "*": ["all",[ "in", "Type","100102","180200","190100","210100","220200","230101","241300","241202"]] }' -F -pC -Z14 -z14''',
            '''{vt} -e {0}/china_poi_15 -l china_poi -j '{ "*": ["all",[ "in", "Type","100105","160100","160301","180202","180601","180602","180603","210204","220300","220301","220302","230103","230104","240600","240700","241203",'"250101","250100","290108"]] }' -F -pC -Z15 -z15'''
        ]
}


def do_layerPoint(layer):
    ftr = layer.ResetReading()
    ftr = layer.GetNextFeature()
    # print('point num:', layer.GetFeatureCount())

    # print('extent:', layer.GetExtent())

    cc = 1
    while ftr:
        # print cc
        cc += 1
        pt = ftr.GetGeometryRef().GetPoint(0)
        g = ftr.GetGeometryRef()
        # print g#,g.ExportKML()
        if isinstance(pt[0], float) is False or isinstance(pt[1], float) is False:
            continue

        if pt[0] > 1000 or pt[1] > 1000:
            pta = pt[0]
            ptb = pt[1]

            while pta > 1000:
                pta = pta / 3600.0
            while ptb > 1000:
                ptb = ptb / 3600.0
            g.SetPoint(0, round(pta, 6), round(ptb, 6))
            # print(pt)
            # print(g)

            '''
            ng = ogr.Geometry(ogr.wkbPoint)
             print pt
            ng.SetPoint(0,pt[0]+40,pt[1])
            ftr.SetGeometry(ng)        
             '''
            layer.SetFeature(ftr)
        ftr = layer.GetNextFeature()


def do_layerLine(layer):
    ftr = layer.ResetReading()
    ftr = layer.GetNextFeature()

    while ftr:
        g = ftr.GetGeometryRef()
        cnt = g.GetPointCount()
        cc = 0
        while cc < cnt:
            # print(g.GetPoint(cc))
            cc += 1

        for n in range(cnt):
            pt = g.GetPoint(n)
            if isinstance(pt[0], float) is False or isinstance(pt[1], float) is False:
                continue
            if pt[0] > 1000 or pt[1] > 1000:
                pta = pt[0]
                ptb = pt[1]

                while pta > 1000:
                    pta = pta / 3600.0
                while ptb > 1000:
                    ptb = ptb / 3600.0
                g.SetPoint(n, round(pta, 6), round(ptb, 6))
        layer.SetFeature(ftr)
        ftr = layer.GetNextFeature()


def merge_dbf(layer, file):
    # ftr = layer.GetNextFeature()
    # 清除历史
    merge_dbf_buff = {}
    # 获取poiname
    path = file.split("/")
    path.remove("POI.dbf")
    path = "/".join(path) + "/PoiName.dbf"
    # print("\n" + path)

    driver = ogr.GetDriverByName('ESRI Shapefile')
    inds = driver.Open(path, 0)  # 0 - read , 1 - write

    poi_name_layer = inds.GetLayer()

    if poi_name_layer.GetFeatureCount() == 0:
        return
    # print("name:" + poi_name_layer.GetName())

    layer_defn = poi_name_layer.GetLayerDefn()
    feature_count = layer_defn.GetFieldCount()

    # 获得poiname字典
    for i in range(0, poi_name_layer.GetFeatureCount()):
        feature = poi_name_layer.GetFeature(i)
        # print(feature)
        fields = {}
        for feat in range(feature_count):
            field = layer_defn.GetFieldDefn(feat)
            field_index = layer_defn.GetFieldIndex(field.GetNameRef())
            # print(field.GetNameRef(), feature.GetField(field_index))
            fields[field.GetNameRef()] = feature.GetField(field_index)

        merge_dbf_buff[i] = fields

        del feature
        del fields
    # print(merge_dbf_buff)

    # 添加到poi
    if layer.GetFeatureCount() == 0:
        return
    # print("name:" + layer.GetName()+"|"+str(layer.GetFeatureCount()))

    layer_defn_l = layer.GetLayerDefn()
    # feature_count_l = layer_defn_l.GetFieldCount()

    # 添加poi name
    for i in range(0, layer.GetFeatureCount()):
        feature = layer.GetFeature(i)
        # print(feature)
        for bu in merge_dbf_buff:
            if feature.GetField("GUID") != merge_dbf_buff[bu]["GUID"]:
                continue
            # print(feature.GetField("GUID"), merge_dbf_buff[bu]["GUID"])
            for gu in merge_dbf_buff[bu]:
                field_index = layer.GetLayerDefn().GetFieldIndex(gu)
                # print("="+str(field_index))

                if field_index < 0:
                    pass

                    # 更新key
                    fie = ogr.FieldDefn(gu, ogr.OFTString)
                    fie.SetWidth(150)
                    layer.CreateField(fie)

                    # print(fie, gu)
                layer.ResetReading()

                if field_index > 0:
                    pass
                    # 更新value
                    # sps = merge_dbf_buff[bu][gu]
                    # print(path)

                    # if merge_dbf_buff[bu][gu] == 'None':
                    #    print(merge_dbf_buff[bu][gu])
                    feature.SetField(field_index, merge_dbf_buff[bu][gu])
                    layer.SetFeature(feature)
                layer.ResetReading()

        del feature

    # print(merge_dbf_buff)
    # 添加poi name 值
    '''for i in range(0, layer.GetFeatureCount()):
        feature = layer.GetFeature(i)
        # print(feature)
        for bu in merge_dbf_buff:
            if feature.GetField("GUID") == merge_dbf_buff[bu]["GUID"]:
                for gu in merge_dbf_buff[bu]:
                    if gu != "GUID":
                        fieldIndex = layer_defn_l.GetFieldIndex(gu)
                        # print(fieldIndex)
                        if fieldIndex > 0:
                            # sss = merge_dbf_buff[bu][gu]
                            feature.SetField(fieldIndex, merge_dbf_buff[bu][gu])
                            layer.SetFeature(feature)
        del feature'''

    inds.SyncToDisk()
    inds.Destroy()

    del merge_dbf_buff
    del driver
    del layer
    del layer_defn_l
    del poi_name_layer
    del layer_defn
    del feature_count


def do_layerDbf_and_setProperties(layer):
    # 属性坐标秒度转换
    layer_defn = layer.GetLayerDefn()
    feature_count = layer_defn.GetFieldCount()

    for feat in range(feature_count):
        field = layer_defn.GetFieldDefn(feat)
        # fields.append(field.GetNameRef())
        # log(str(fields))
        # for a in fields:
        name = field.GetNameRef()

        # source_shp_properties_filer 坐标转换
        for pro in source_shp_properties_filer:
            if name.lower() != pro.lower():
                continue

            # 获取所需字段的序号index
            # print(pro)
            field_index = layer_defn.GetFieldIndex(pro)

            # print(fieldIndex)
            for i in range(0, layer.GetFeatureCount()):
                feature = layer.GetFeature(i)
                # 新的属性内容
                code = feature.GetField(field_index)
                if isinstance(code, float) is False:
                    continue
                while code > 1000:
                    code = code / 3600.0
                feature.SetField(field_index, round(code, 6))
                layer.SetFeature(feature)

    del layer_defn
    del feature_count
    # 合并poi+poiname
    # print("\n" + layer.GetName())


def do_layerPolygon(layer):
    ftr = layer.ResetReading()
    ftr = layer.GetNextFeature()

    while ftr:
        g = ftr.GetGeometryRef()
        if hasattr(g, "GetGeometryCount") is False:
            break
        cnt = g.GetGeometryCount()

        for n in range(cnt):
            gg = g.GetGeometryRef(n)
            for m in range(gg.GetPointCount()):
                pt = gg.GetPoint(m)
                # print(pt)
                if isinstance(pt[0], float) is False or isinstance(pt[1], float) is False:
                    continue
                if pt[0] > 1000 or pt[1] > 1000:
                    pta = pt[0]
                    ptb = pt[1]
                    while pta > 1000:
                        pta = pta / 3600
                    while ptb > 1000:
                        ptb = ptb / 3600
                    gg.SetPoint(m, round(pta, 6), round(ptb, 6))
        layer.SetFeature(ftr)
        ftr = layer.GetNextFeature()


def do_shpfile(file):
    layer_name = file.split("/")[-1].split(".")[0]

    # if shp_filter(layer_name) is False:
    #   return

    driver = ogr.GetDriverByName('ESRI Shapefile')
    # log(file)

    inds = driver.Open(file, 1)  # 0 - read , 1 - write
    layer = inds.GetLayer()

    if layer.GetFeatureCount() == 0:
        return
    gtype = layer.GetLayerDefn().GetGeomType()

    if gtype == ogr.wkbPoint:
        # print("=== type:wkbPoint ===")
        do_layerPoint(layer)
    elif gtype == ogr.wkbLineString:
        # print("=== type:wkbLineString ===")
        do_layerLine(layer)
    elif gtype == ogr.wkbPolygon:
        # print("=== type:wkbPolygon ===")
        do_layerPolygon(layer)
    # elif gtype == 100:
    # pass
    # merge_dbf(layer, file)
    else:
        log('unknown type:' + str(file))

    # 统一属性秒度转换
    do_layerDbf_and_setProperties(layer)

    inds.SyncToDisk()
    inds.Destroy()

    del inds
    del layer
    del driver
    del layer_name


def convert(shpdir):
    shpdir = file_path + "/" + shpdir
    files = os.listdir(shpdir)
    # log(shpdir)

    for file in files:
        # 普通shp文件处理
        if file.lower().find('.shp') > -1:
            # print(shpdir + "/" + file)
            # try:
            file = shpdir + "/" + file
            layer_name = file.split("/")[-1].split(".")[0]

            # 排除那些无需转换的项
            if layer_name in source_shp_name_filter:
                continue

            # 秒转度
            do_shpfile(file)

            # 转换到geojson
            fackname = json_name.get(layer_name)
            to_geojson(file, file.split(layer_name)[0] + "/" + str(fackname) + ".json")
        # except Exception as e:
        # error(str(e))
        # pass

        # 加入dbf文件处理
        '''
        for t in source_shp_type_filter:
            if file.lower().find(t.lower()) > -1:
                # print(shpdir + "/" + t)

                try:
                    file = shpdir + "/" + t
                    layer_name = file.split("/")[-1].split(".")[0]

                    # 秒转度
                    do_shpfile(file)

                    # 转换到geojson
                    to_geojson(file, file.split(layer_name)[0] + "/" + layer_name + ".geojson")
                except Exception as e:
                    log(shpdir + "/" + t)
                pass
        '''


def create_dir():
    isExists = os.path.exists(out_path)
    if not isExists:
        os.mkdir(out_path)


# 检测排除不需要转换的shp
def shp_filter(name):
    boo = True
    for filename in source_shp_name_filter:
        if filename.lower() == name.lower():
            boo = False
    return boo


def to_geojson_ogr2ogr(vector, output):
    # 打开矢量图层
    if os.path.isfile(output):
        os.remove(output)
    js = '''ogr2ogr -f "GeoJSON" {out} {in} '''.replace("{in}", vector).replace("{out}", output)
    # print("\t \r geojson")
    os.system(js)


def to_geojson(vector, output):
    # 检测文件是否存在
    if os.path.isfile(output):
        os.remove(output)

    # 打开矢量图层
    gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "YES")
    gdal.SetConfigOption("SHAPE_ENCODING", "GBK")
    shp_ds = ogr.Open(vector)
    shp_lyr = shp_ds.GetLayer(0)

    # 创建结果Geojson
    base_name = os.path.basename(output)
    out_driver = ogr.GetDriverByName('GeoJSON')
    out_ds = out_driver.CreateDataSource(output)
    if out_ds.GetLayer(base_name):
        out_ds.DeleteLayer(base_name)

    out_lyr = out_ds.CreateLayer(base_name, shp_lyr.GetSpatialRef())
    out_lyr.CreateFields(shp_lyr.schema)
    out_feat = ogr.Feature(out_lyr.GetLayerDefn())

    # 生成结果文件
    for feature in shp_lyr:
        # 根据需要的数据输出
        out_feat.SetGeometry(feature.geometry())
        for j in range(feature.GetFieldCount()):
            # 移除特殊字符
            fg = str(feature.GetField(j))
            if fg is None:
                fg = ""
            out_feat.SetField(j, fg.replace("^", "").replace("None", ""))

            del fg
        out_lyr.CreateFeature(out_feat)

    del out_ds
    del shp_ds


# 进度
def process_bar(percent, start_str='', end_str='', total_length=0):
    bar = ''.join(["▋"] * int(percent * total_length)) + ''
    bar = '\r' + start_str + " " + bar.ljust(total_length) + '  {:0>4.4f}%|'.format(percent * 100) + end_str
    os.write(1, bar.encode())
    sys.stdout.flush()
    # print(bar, end='', flush=False)


def log(text_log):
    text_log = "\n " + str(text_log)
    print(text_log, end='', flush=False)

    data = open(file_path + "/log_" + time.strftime("%Y-%m-%d", time.localtime()) + ".log", "a+", encoding="utf-8")
    print(text_log + "|" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), file=data)
    data.close()


def error(text_log):
    text_log = "\n " + str(text_log)
    # time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(text_log, end='', flush=False)

    data = open(file_path + "/error_" + time.strftime("%Y-%m-%d", time.localtime()) + ".log", "a+", encoding="utf-8")
    print(text_log + "|" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), file=data)
    data.close()


# 合并geojson
def union_geojson(f_paths):
    letter = {}

    for fp in f_paths:
        # print(os.listdir(fp))
        fp_arr = os.listdir(file_path + "/" + fp)
        fp_arrs = []

        # print(fp)
        for f in fp_arr:
            if f.split(".")[0] not in json_name.values():
                continue
            if f.lower().find(".json") > -1:
                fp_arrs.append(f)
        letter[fp] = fp_arrs

        del fp_arrs
        del fp_arr
    # print(letter)
    return letter


# 切图导出库
def exportsh(name, files, tipp):
    index = 0
    vt = os.getcwd() + "/vt/bin/vt"
    for ti in tipp:
        index += 1
        data = open(file_path + "/" + name + "_" + str(index) + ".sh", "w+", encoding="utf-8")
        print(ti.replace("{0}", out_path).replace("{vt}", vt) + " " + str(files), file=data)
        data.close()


# 拼接切图
def bind_tile_code(union_map):
    index = 0

    # 获得单个字典 Bind tile code
    tiles = {}
    for ii in json_name:
        tiles[ii] = ""

    union_map_index = 0
    # 填充字典
    for um in union_map:
        union_map_index = union_map_index + 1
        # print(union_map_index,len(union_map))
        # 进度显示
        process_bar(round(100 / len(union_map), 6) * union_map_index / 100, start_str='[tile processing]', end_str=' ',
                    total_length=50)

        for umu in union_map[um]:

            if umu.split(".")[0] not in json_name.values():
                continue
            index += 1
            # 替换简写
            umu_char = umu.split(".")[0]
            umu_new = list(json_name.keys())[list(json_name.values()).index(umu_char)]
            # if json_name.get(umu_char) in json_name:
            #    umu_char = json_name.get(umu_char) + ".json"
            tiles[umu_new] = tiles[umu_new] + " " + um + "/" + umu_char + ".json"

    log("检索到共计[" + str(index) + "]块图块")

    # 合并省市县字典
    # pccc = ""
    # pccc_key = "ProvinceCityCountryCounty"

    for kt in tiles:
        '''if kt in pccc_key:
            pccc += " " + tiles[kt]
        else:
            if len(tipp_rules[kt]) == 0:
                continue'''
        exportsh(kt, tiles[kt], tipp_rules[kt])

    # 省市县独立导出
    # exportsh(pccc_key, pccc, tipp_rules[pccc_key])

    del tiles
    del union_map


def run_vector_production():
    # 进入导出路径
    os.chdir(file_path)

    commands = os.listdir(file_path)
    command_array = []
    command_index = 0
    for c in commands:
        # 普通shp文件处理
        if c.lower().find('.sh') <= -1:
            continue
        command_index += 1
        process_bar(round(100 / len(commands), 6) * command_index / 100, start_str='[sh processing]', end_str=' ',
                    total_length=50)
        # command_array.append(c)
        # p = subprocess.Popen("sh " + str(c), shell=True)
        # p.wait()
        os.system("sh " + str(c))
    # log(command_array)

    log("脚本队列执行结束")

    pass


# 组织编码
def shell_production(paths):
    # os.system("pwd")
    # os.system("tippecanoe -v")

    # 合并geojson
    union_map = union_geojson(paths)

    # 绑定不同层级处理规则
    bind_tile_code(union_map)


def shp_to_json(path):
    index = 0
    for p in path:
        index = index + 1

        # 进度显示
        process_bar(round(100 / len(path), 6) * index / 100, start_str='[tojson processing]', end_str=' ',
                    total_length=50)

        # 处理开始
        # threa = threading.Thread(target=convert, args=(p,))
        # threa.start()
        convert(p)

    log("共转换[" + str(index) + "]个shp文件")
    del path


def check_dir(path):
    log("in path (SHP): " + path)
    log("out path(PBF): " + out_path)

    dir_list = os.listdir(path)
    # 字典记录当前目录
    dir_dictionary = {}
    dir_dir_dictionary_count = 0
    for d in dir_list:
        if os.path.isdir(path + "/" + d):
            dir_dir_dictionary_count += 1
            dir_dictionary[d] = os.listdir(path + "/" + d)

    # log("作业目录: 共计[" + str(dir_dir_dictionary_count) + "]个")

    # 作业开始
    shp_file_path = []
    for d_d in dir_dictionary:
        for dd_d in dir_dictionary[d_d]:
            shp_file_path.append(d_d + "/" + dd_d)

    # 输入log记录
    data = open(file_path + "/MESH.log", "a+", encoding="utf-8")
    print(str(shp_file_path), file=data)
    data.close()

    return shp_file_path


def state_write(status, progress):
    pass
    # 进度写入
    job_json['Userproperty']['OutputParameter']['ProgramStatus'][2]['value'] = str(status)

    # 运行状态
    job_json['Userproperty']['OutputParameter']['ProgramStatus'][0]['value'] = str(progress)

    # 写入
    with open(os.getcwd() + "/job_order.json", "w") as f:
        json.dump(job_json, f)


def state_read():
    pass
    with open(os.getcwd() + "/job_order.json", 'r', encoding='utf8')as fp:
        global job_json
        job_json = json.load(fp)

    # log("读取job_order成功")


def init_run():
    log("*************** 作业开始 *************")
    state_write(1, 1)

    # 执行批量转换
    log("[1]源数据文件检查 开始")
    shp_file_path_arr = check_dir(file_path)
    state_write(1, 10)

    # 执行批量转换
    log("[2]秒度转换json转换 开始")
    shp_to_json(shp_file_path_arr)
    state_write(1, 20)

    # 执行切片执行语句组织
    log("[3]生产切片执行语句 开始")
    shell_production(shp_file_path_arr)
    state_write(1, 30)

    # 执行切片生产任务
    log("[4]切片语句代码执行 开始")
    run_vector_production()
    state_write(0, 100)
    print("\r 任务完成")
    log("任务完成")


if __name__ == '__main__':
    # if sys.argv[1:]:
    # 读取json
    state_read()

    # 获取输入文件路径
    global file_path
    file_path = job_json['Userproperty']['InputParameter']['InputFilePath'][0]['value']

    # 获取输出路径
    global out_path
    out_path = job_json['Userproperty']['OutputParameter']['OutputFilePath'][0]['value']

    # 程序运行
    init_run()

    # else:
    # 默认地址测试使用
    # log("没有进入程序")
