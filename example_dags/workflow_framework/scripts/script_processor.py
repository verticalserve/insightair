# Copyright 2022 VerticalServe INC
# InsightAir Workflow Framework - Config Driven Airflow Framework
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import subprocess
from optparse import OptionParser


# Script Processor to handle parameterized scripts, it replaces the passed parameters and invokes the script.
# It then writes the results to an output file

parser = OptionParser()
parser.add_option("-s", "--script", dest="script_file",
                  help="Script file")
parser.add_option("-o", "--output", dest="output_file",
                  help="Output file")
parser.add_option("-p", "--params", dest="params",
                  help="Parameters")

(options, args) = parser.parse_args(sys.argv)

print(options.params)
param_list = options.params.split(',')

# Open the parsed script
script = ''
with open(options.script_file, "rb") as f:
    script = f.read().decode("utf-8")
    print(script)

# Replace parameters
for param in param_list:
    kv = param.split('=')
    script = script.replace('$' + kv[0], kv[1])

print(script)

# Execute the script
result = subprocess.run(script, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
print(result.returncode, result.stdout.decode("utf-8"), result.stderr)

# Write the output to a temp file
if result.returncode == 0:
    output = result.stdout.decode("utf-8")
    with open('/tmp/' + options.output_file, 'w') as f:
        f.write(output)