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
from datetime import datetime


def to_query(params: dict) -> str:
    """ Returns the given dictionary as a string with the format
    `key1=value1&...&keyN=valueN`.
    For use with `HTTP GET` query strings."""
    return str.join('&', [f"{k}={v}" for k, v in params.items()])


def to_lucene(date: datetime) -> str:
    """ Returns the given date as a string with the format %Y%m%dT%H%M%S
    for use with lucene query syntax."""
    return date.strftime("%Y%m%dT%H%M%S")
