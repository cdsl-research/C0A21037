# pexpectライブラリからpxsshモジュールをインポート
from pexpect import pxssh
import re
import subprocess
import datetime #日付
import numpy as np
import pandas as pd
from pathlib import Path #ファイルあるかないか
import csv



# ログイン情報を設定しSSHサーバーにログイン
ssh = pxssh.pxssh()
ssh.login(server="", #接続したいSSHサーバーのIPを記述
                  username="", #SSHサーバー側のユーザー名を記述
                            password="") #SSHサーバー側のユーザーのパス

# カレントディレクトリのファイルを表示する


#バックアップファイルの日付取得
ssh.sendline('ls /volume1/jasmine2-backup/backup')
ssh.expect(r"\[.*\]\$ ")
day=ssh.after.decode(encoding='utf-8')
day2=(re.findall(r'20\d*-\d*-\d*-\d*-\d*',day))
day2.sort()

#初回起動時か確認
#csvを作る
file_path_str = "test.csv"
# 文字列を Path オブジェクトに変換
file_path_obj = Path(file_path_str)
if not file_path_obj.exists():
    file_path_obj.touch()

    #初回起動時
    #バックアップファイル内の全てのvm名取得
    vmname2=[]
    for i in range(len(day2)):
            ssh.sendline("ls /volume1/jasmine2-backup/backup/"+day2[i])
            ssh.expect(r"\[.*\]\$ ")
            vmname=ssh.after.decode(encoding='utf-8')
            vmname3=(re.findall(r'm([0-9a-zA-Z].*?)\x1b\S*',vmname))
            for j in range(len(vmname3)):
                if vmname3[j] not in vmname2:#無いとき
                    vmname2.append(vmname3[j])
    #圧縮済みを消す
    kesu=[i for i, x in enumerate(vmname2) if ".tar.gz" in x]
    popcou=0
    for i in range(len(kesu)):
        vmname2.pop(kesu[i]-popcou)
        popcou+=1
        
    # NumPyの配列に変換
    vmnum = np.array(vmname2, dtype='str')
    # csv2に書き込み
    np.savetxt('vmname.csv', vmnum, fmt='%s', delimiter=',')
    

    #データ量取得
    vmlis=[]

    for j in range(len(vmname2)):
        hokan=0
        for i in range(len(day2)):
            komando="du /volume1/jasmine2-backup/backup/"+day2[i]+"/"+vmname2[j]+"/"+vmname2[j]+"-flat.vmdk"
            if i==0:
                #実行コマンド
                ssh.sendline(komando)
                ssh.expect(r"\[.*\]\$ ")
                du1=ssh.before.decode(encoding='utf-8')
                #データ量
                du2=(re.findall(r'(\d*)\t',du1 ))

                #リストに格納
                vmlis.append(vmname2[j])
                if not du2:
                    du3=ssh.after.decode(encoding='utf-8')
                    du4=(re.findall(r'(\d*)\t',du3))
                    if not du4:
                        #vmlis.append(hokan)
                        vmlis.append(0)
                    else:
                        vmlis.append(int(du4[0]))
                    continue
                vmlis.append(int(du2[0]))
                hokan=vmlis[-1]
            else:
                #実行コマンド
                ssh.sendline(komando)
                ssh.expect(r"\[.*\]\$")
                du1=ssh.before.decode(encoding='utf-8')
                #データ量
                du2=(re.findall(r'(\d*)\t',du1 ))
                #print(du1)
                #リストに格納
                if not du2:
                    du3=ssh.after.decode(encoding='utf-8')
                    du4=(re.findall(r'(\d*)\t',du3))
                    if not du4:
                        vmlis.append(hokan)
                        #vmlis.append(0)
                    else:
                        vmlis.append(int(du4[0]))
                    continue
                vmlis.append(int(du2[0]))
                hokan=vmlis[-1]
    #print(vmlis)

    # NumPyの配列に変換
    mixed_array = np.array(vmlis, dtype='object')
    # csv2に書き込み
    np.savetxt('test.csv', mixed_array, fmt='%s', delimiter=',')
    
    # NumPyの配列に変換
    vmnum = np.array(vmname2, dtype='str')
    # csv1に書き込み
    np.savetxt('vmname.csv', vmnum, fmt='%s', delimiter=',')
    
  
#2回目以降
else:
    #前日までのリストを呼び出す
    with open('test.csv', 'r') as deta:
        csv_reader = csv.reader(deta)
        vmlis=[element for row in csv_reader for element in row]
    
    #新しいVMが作成されていた場合追加する
    
    with open('vmname.csv', 'r') as count:
        csv_reader = csv.reader(count)
        vmname2 =[element for row in csv_reader for element in row]

    ssh.sendline("ls /volume1/jasmine2-backup/backup/"+day2[-1])
    ssh.expect(r"\[.*\]\$ ")
    vmname=ssh.after.decode(encoding='utf-8')
    vmname3=(re.findall(r'm([0-9a-zA-Z].*?)\x1b\S*',vmname))
    
    #圧縮済みを消す
    kesu=[i for i, x in enumerate(vmname3) if ".tar.gz" in x]
    popcou=0
    for i in range(len(kesu)):
        vmname3.pop(kesu[i]-popcou)
        popcou+=1
    for i in range(len(vmname3)):
        if vmname3[i] not in vmname2:#無いとき
            vmname2.append(vmname3[i])
            vmlis.append(vmname3[i])
            #print(vmname3[i])
            for j in range(len(day2)):
                vmlis.append(0)
    
    # NumPyの配列に変換
    mixed_array = np.array(vmlis, dtype='object')
    # csv2に書き込み
    np.savetxt('test.csv', mixed_array, fmt='%s', delimiter=',')
    
    # NumPyの配列に変換
    vmnum = np.array(vmname2, dtype='str')
    # csv1に書き込み
    np.savetxt('vmname.csv', vmnum, fmt='%s', delimiter=',')
    
    #一番古い日付のデータ量を消す
    del vmlis[1::len(day2)+1]

    new_deta=[]
    #最新のデータ量を取る
    for i in range(len(vmname2)):
        komando="du /volume1/jasmine2-backup/backup/"+day2[-1]+"/"+vmname2[i]+"/"+vmname2[i]+"-flat.vmdk"
        ssh.sendline(komando)
        ssh.expect(r"\[.*\]\$")
        new_deta1=ssh.before.decode(encoding='utf-8')
        #データ量
        new_deta2=(re.findall(r'(\d*)\t',new_deta1 ))
        
        #リストに格納
        if not new_deta2:
            new_deta3=ssh.after.decode(encoding='utf-8')
            new_deta4=(re.findall(r'(\d*)\t',new_deta3))
            if not new_deta4:
                hokan1=vmlis.index(vmname2[i])
                new_deta.append(vmlis[hokan1+len(day2)-1])
                #new_deta.append(0)
            else:
                new_deta.append(int(new_deta4[0]))
            continue
        new_deta.append(int(new_deta2[0]))

    #print(new_deta)
        
    #vmlisに入れていく
    tyousetu=0
    for i in range(len(new_deta)-1):
        vmlis.insert(len(day2)*(tyousetu+1)+tyousetu,new_deta[i])
        tyousetu+=1
    vmlis.append(new_deta[-1])

    # NumPyの配列に変換
    mixed_array = np.array(vmlis, dtype='object')
    # csv2に書き込み
    np.savetxt('test.csv', mixed_array, fmt='%s', delimiter=',')  

        
#相関を取るvmAを別リストに入れる
soukanvm=[""]
soukan = vmlis.index("")
for i in range(len(day2)):
    vmlis.pop(soukan)
    soukanvm.append(int(vmlis[soukan]))
vmlis.pop(soukan)
 
 
#データの変化量が0＝シャットダウンしているので圧縮
asshuku=[]
st=1
en=len(day2)+1
asshuku=[]
for i in range(len(vmname2)-1):
    s21=vmlis[st:en]
    print(s21)
    s2 = list(map(int, s21))
    if(all(i == s2[0] for i in s2)):
        asshuku.append(vmlis[st-1])
        del vmlis[st-1:en]
        st-=len(day2)+1
        en-=len(day2)+1
    st+=len(day2)+1
    en+=len(day2)+1
    #print(vmlis)
   
#相関の計算
st=1
en=len(day2)+1
for i in range(len(vmname2)-1):
    
# pandasを使用してPearson's rを計算
    s1=pd.Series(soukanvm[1:len(day2)+1])
    s2=pd.Series(vmlis[st:en])


    res=s1.corr(s2)   # numpy.float64 に格納される
    
    if (res>=0.97):   
        asshuku.append(vmlis[st-1])
        #print(vmlis[st-1])
        #print(res)
        
    st+=len(day2)+1
    en+=len(day2)+1
print("圧縮するフォルダ") 
print(asshuku)
    
#圧縮する

for j in range(len(asshuku)):
    
    for i in range(len(day2)):
        #フォルダがあるか，あれば圧縮
        komando="find /volume1/jasmine2-backup/backup/"+day2[i]+"/"+asshuku[j]
        ssh.sendline(komando)
        ssh.expect(r"\[.*\]\$ ")
        asshuku1=ssh.before.decode(encoding='utf-8')
        asshuku2=(re.findall(r'No such file or directory',asshuku1))
        if not asshuku2:    
            komando="tar -zcvf /volume1/jasmine2-backup/backup/"+day2[i]+"/"+asshuku[j]+".tar.gz /volume1/jasmine2-backup/backup/"+day2[i]+"/"+asshuku[j]
            ssh.sendline(komando)
            ssh.expect(r"\[.*\]\$ ",timeout=828000)

    komando="ls /volume1/jasmine2-backup/backup/"+day2[i]
    ssh.sendline(komando)  
    ssh.expect(r"\[.*\]\$ ")  
    asshuku1=ssh.after.decode(encoding='utf-8')
    asshuku2=(re.findall(r'm([0-9a-zA-Z].*?)\x1b\S*',asshuku1))
    #print(asshuku2)



# SSHサーバーからログアウト
ssh.logout()

