
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM java:8

ENV app /kafka-manager
WORKDIR ${app}

COPY sbt $app/sbt
COPY build.sbt $app/build.sbt
COPY app $app/app
COPY conf $app/conf
COPY img $app/img
COPY project/build.properties $app/project/build.properties
COPY project/plugins.sbt $app/project/plugins.sbt
COPY public $app/public
COPY src $app/src
COPY test $app/test

RUN ./sbt assembly
EXPOSE 9000
CMD [ "./sbt", "run" ]
