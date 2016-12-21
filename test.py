import subprocess32 as subprocess
import time
def bash(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = p.communicate()
    if result[1] or p.returncode:
        return_code = 1
    else:
        return_code = 0
    output = "stdout:\n%s\nstderr:\n%s" % (result[0], result[1])
    return {"code": return_code, "output": output}


cmd='zcat /test_db_backup/jubao_2016-12-15_master-bin.000018_172198131.sql.gz | head -10'
result = bash(cmd)
if result["code"] == 0:
    print("return code is 0\n%s" % result["output"])
else:
    print("return code is not 0\n%s" % result["output"])

