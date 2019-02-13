# ======================================================================= #
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        #
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     #
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. #
#  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      #
#  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  #
#  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  #
#  OTHER DEALINGS IN THE SOFTWARE.                                        #
#                                                                         #
#  For more information, please refer to <http://unlicense.org>           #
# ======================================================================= #
import logging
import sys

from dragonfly import dragonfly

LOG_ENTRY_FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(
    filename='log.txt', level=logging.DEBUG, format=LOG_ENTRY_FORMAT)

if __name__ == "__main__":
    config_filename = sys.argv[1]

    sync = dragonfly.Sync(config_filename)
    total = sync.run()
    summary = f"Synced {total} records!"
    logging.info(summary)
    print(summary)
