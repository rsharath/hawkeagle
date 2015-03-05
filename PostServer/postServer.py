import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.gen
from tornado.web import RequestHandler, Application, url
from urlparse import urlparse, parse_qs

import json
import datetime
import pymongo
import sys

# Setup Tornado Options
from tornado.options import define, options
define("port", default=8000, help="run on the given port", type=int)
# define mongodb host
# define mongodb db
# define mongodb user
# define mongodb passwd
# Global connection to MongoDB
try:
    gConn = pymongo.MongoClient()
except:
    print "Error: initializing MongoDB..."
    #raise

#
# TODO: fix this later to return ERROR value
# Get all the control parameters from the URL
# todo: always requires trailing in POST header / for now
#
def getControlParams(params):
    # first get the path header
    parseparam = params.path
    if parseparam.startswith('/') and parseparam.endswith('/'):
        parseparam = parseparam[1:-1]
        path_list = parseparam.split('/')
    # leading '/'
    elif parseparam.startswith('/'):
        parseparam = parseparam[1:]
        path_list = parseparam.split('/')
    # trailing '/'
    elif parseparam.endswith('/'):
        parseparam = parseparam[0:-1]
        path_list = parseparam.split('/')
    # no '/'
    else:
        # return error message here, fix todo, dont call this func multiple times
        return None, None

    # return results
    return path_list[0], path_list[2]


#
# Base Application
#
class Application(tornado.web.Application):
    def __init__(self):
        # Setup Handlers
        handlers = [
            url(r"/projects/(.*)", ProjectsHandler),
        ]

        # Setup Settings
        settings = dict (
            # define later
        )

        # Init Application
        tornado.web.Application.__init__(self, handlers, settings)


#
# Project Handler
#
class ProjectsHandler(tornado.web.RequestHandler):
    def post(self, input):
        try:
            #print "GOT THIS: "+self.request.body

            # Get POST headers
            project, bucket = getControlParams(urlparse(input))
            if (bucket is None):
                self.set_status(400, "No record to process")
                return

            # Synthesize Control information
            controlInfo = {}
            controlInfo['_hawkeagle_'] = {}
            controlInfo['_hawkeagle_']['_timestamp'] = datetime.datetime.now().isoformat()
            controlInfo['_hawkeagle_']["_project"] = project
            controlInfo['_hawkeagle_']["_bucket"] = bucket

            # Get POST payload
            payload = self.request.body
            if (payload is None):
                self.set_status(400, "No record to process")
                return

            print "INSERT " + str(payload) + str(controlInfo)

            # Serialize to dict object which will be inserted into the database
            writedata = {}
            writedata = json.loads(payload)

            # Add Control information to payload that will be written to database
            writedata.update(controlInfo)


            # Write to DB
            try:
                db = gConn['EventsDB']
                collection = db[project+'__'+bucket]
                eventID = collection.insert(writedata)

                # Success!
                self.write({"success": True})

            except:
                # DB Failure!
                self.write({"success": False})
                print 'Error: '+str(sys.exc_info()[0])+': DB Write Failure - check if event-database exists'
                self.set_status(503, "Failed to write to DB")

        except:
            # Failure!
            self.write({"success": False})
            print 'Error: '+ str(sys.exc_info()[0])

            self.set_status(503)

#
# Main
#
def main():
    try:
        tornado.options.parse_command_line()
        http_server = tornado.httpserver.HTTPServer(Application())
        http_server.listen(options.port)
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        print "Stopping Tornado..."
        tornado.ioloop.IOLoop.instance().stop()
    except:
        print "Stopping Tornado..."
        tornado.ioloop.IOLoop.instance().stop()

#
# Init Prog
#
if __name__ == "__main__":
    main()



