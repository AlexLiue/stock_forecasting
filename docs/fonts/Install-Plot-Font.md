# 安装中文字体

matplotlib 不支持中文

## MacOS 系统： 

### Step 1 : 找到 matplotlib 字体库目录[/matplotlib/mpl-data/fonts/ttf], 拷贝 SimHei.ttf 到字体目录 
```shell
(base)$ pwd
/Users/alex/opt/anaconda3/lib/python3.9/site-packages/matplotlib/mpl-data/fonts/ttf
(base)$  ls -lrt SimHei.ttf
-rw-r--r--  1 alex  staff  10050868  4 18 19:45 SimHei.ttf
```

### Step 2: 清理缓存
```shell
rm -rf  ~/.matplotlib
```
