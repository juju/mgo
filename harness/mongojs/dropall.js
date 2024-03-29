
var ports = [40001, 40002, 40011, 40012, 40013, 40021, 40022, 40023, 40041, 40101, 40102, 40103, 40201, 40202, 40203]
var auth = [40002, 40103, 40203, 40031]
var db1 = new Mongo("localhost:40001")

if (db1.getDB("admin").serverBuildInfo().OpenSSLVersion) {
    ports.push(40003)
    auth.push(40003)
}

for (var i in ports) {
    var port = ports[i]
    var server = "localhost:" + port
    var mongo = null
    for (var j = 0; j < 10; j++) {
        try {
            mongo = new Mongo("localhost:" + port)
        } catch (err) {
            if (j+1 == 10) {
                throw err
            }
            print("failed to connect to " + server + " retrying in 1s ")
            sleep(1000)
        }
    }
    var admin = mongo.getDB("admin")

    for (var j in auth) {
        if (auth[j] == port) {
            print("removing user for port " + auth[j])
            for (var k = 0; k < 10; k++) {
                var ok = admin.auth("root", "rapadura")
                if (ok) {
                    admin.system.users.find().forEach(function (u) {
                        if (u.user == "root" || u.user == "reader") {
                            return;
                        }
                        if (typeof admin.dropUser == "function") {
                            mongo.getDB(u.db).dropUser(u.user);
                        } else {
                            admin.removeUser(u.user);
                        }
                    })
                    break
                }
                print("failed to auth for port " + port + " retrying in 1s ")
                sleep(1000)
            }
        }
    }
    var result = admin.runCommand({ "listDatabases": 1 })
    for (var j = 0; j != 100; j++) {
        if (typeof result.databases != "undefined" || notMaster(result)) {
            break
        }
        result = admin.runCommand({ "listDatabases": 1 })
    }
    if (notMaster(result)) {
        continue
    }
    if (typeof result.databases == "undefined") {
        print("Could not list databases. Command result:")
        print(JSON.stringify(result))
        quit(12)
    }
    var dbs = result.databases
    for (var j = 0; j != dbs.length; j++) {
        var db = dbs[j]
        switch (db.name) {
            case "admin":
            case "local":
            case "config":
                break
            default:
                mongo.getDB(db.name).dropDatabase()
        }
    }
}

function notMaster(result) {
    return typeof result.errmsg != "undefined" && (result.errmsg.indexOf("not master") >= 0 || result.errmsg.indexOf("no master found"))
}

// vim:ts=4:sw=4:et
