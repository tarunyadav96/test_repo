import os
from sys import argv
import shutil

##unzipPyPath = argv[0]
##input_dir = argv[1]
##delete_source = argv[2]
#if len(argv) > 3:
    #CreateFolder = argv[3]
#else:
   #CreateFolder = 0
    
unzipPyPath = ""
input_dir = r"D:\Raw Data\Transtat\files"
delete_source = 0
CreateFolder = 0
CreateFolder = int(CreateFolder)

for root, dirs, files in os.walk(input_dir):
    if len(files) > 0:
        for filename in files:
            opath = ""
            try:
                print("Processing: " + filename)
                full_file_path = os.path.join(root, filename)
                fname = filename
#                if (CreateFolder == 1) or (CreateFolder == 2):
                if (CreateFolder == 1) :
                    opath = root + "\\" + fname.replace(".","_")
                if not os.path.exists(opath):
                    if opath != "":                        
                        os.makedirs(opath)
                    else:
                        opath = root
                else:
                    opath = root
                if filename.endswith('.zip'):
                    import zipfile
                    zip_ref = zipfile.ZipFile(full_file_path, 'r')
                    zip_ref.extractall(opath)
                    zip_ref.close()
                if filename.endswith('tar.gz'):
                    import tarfile
                    tar = tarfile.open(full_file_path, 'r')
                    tar.extractall(opath)
                    tar.close()
                if filename.endswith('.gz'):
                    import gzip
                    print(opath+"\\"+filename[:-3])
                    with gzip.open(full_file_path, 'rb') as f_in:
                        open(opath+"\\"+filename[:-3],'wb').write(f_in.read())
                if filename.endswith('.7z'):
                    from pyunpack import Archive
                    Archive(full_file_path).extractall(opath)
                if int(delete_source) ==1:
                    try:
                        os.remove(root+"\\"+filename)
                    except OSError:
                        pass
            except Exception as e:
                print("error catch")
                print(e)
                continue
            if (CreateFolder == 2) and (opath != ""):
                fldlst = []
                for root1,dir1,file1 in os.walk(opath):
                    if len(file1) > 0:
                        for filename1 in file1:                            
                            if root1 != opath:
                                currfilepath = root1 + "\\" + filename1 
                                destfilepath = opath + "\\" + filename1
                                filecnt = 1
                                while os.path.exists(destfilepath):
                                    destfilepath = opath + "\\" + os.path.splitext(filename1)[0] + "____" + str(filecnt) + os.path.splitext(filename1)[1]
                                    filecnt = filecnt + 1
                                shutil.copyfile(currfilepath,destfilepath)
                                fldlst.append(root1)
                    else:
                        if root1 != opath:
                            fldlst.append(root1)
                fldlst = list(set(fldlst))
                for fld in fldlst:
                    if os.path.exists(fld):
                        shutil.rmtree(fld)
