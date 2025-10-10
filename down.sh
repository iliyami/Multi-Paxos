lsof -ti:8001,8002,8003,8004,8005 | xargs kill -9 2>/dev/null; sleep 3 && echo -e 2n1.1n3.1n3.2n3.3n5.1nn2n1.1n3.1n3.2n3.3n5.1nn2n1.1n3.1n3.2n3.3n5.1nn2n1.1n3.1n3.2n3.3n3.4n5.1nn
