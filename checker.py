2026-03-10T08:31:46.1484245Z Current runner version: '2.332.0'
2026-03-10T08:31:46.1512964Z ##[group]Runner Image Provisioner
2026-03-10T08:31:46.1514359Z Hosted Compute Agent
2026-03-10T08:31:46.1515357Z Version: 20260213.493
2026-03-10T08:31:46.1516319Z Commit: 5c115507f6dd24b8de37d8bbe0bb4509d0cc0fa3
2026-03-10T08:31:46.1517413Z Build Date: 2026-02-13T00:28:41Z
2026-03-10T08:31:46.1518451Z Worker ID: {fd8073a7-9dad-4db8-9c5c-d0384ce1de71}
2026-03-10T08:31:46.1519596Z Azure Region: northcentralus
2026-03-10T08:31:46.1520459Z ##[endgroup]
2026-03-10T08:31:46.1522426Z ##[group]Operating System
2026-03-10T08:31:46.1523790Z Ubuntu
2026-03-10T08:31:46.1524616Z 24.04.3
2026-03-10T08:31:46.1525499Z LTS
2026-03-10T08:31:46.1526268Z ##[endgroup]
2026-03-10T08:31:46.1527153Z ##[group]Runner Image
2026-03-10T08:31:46.1527952Z Image: ubuntu-24.04
2026-03-10T08:31:46.1528915Z Version: 20260302.42.1
2026-03-10T08:31:46.1530593Z Included Software: https://github.com/actions/runner-images/blob/ubuntu24/20260302.42/images/ubuntu/Ubuntu2404-Readme.md
2026-03-10T08:31:46.1532784Z Image Release: https://github.com/actions/runner-images/releases/tag/ubuntu24%2F20260302.42
2026-03-10T08:31:46.1534054Z ##[endgroup]
2026-03-10T08:31:46.1535761Z ##[group]GITHUB_TOKEN Permissions
2026-03-10T08:31:46.1538076Z Contents: read
2026-03-10T08:31:46.1538956Z Metadata: read
2026-03-10T08:31:46.1539798Z Packages: read
2026-03-10T08:31:46.1540563Z ##[endgroup]
2026-03-10T08:31:46.1543475Z Secret source: Actions
2026-03-10T08:31:46.1544583Z Prepare workflow directory
2026-03-10T08:31:46.2006922Z Prepare all required actions
2026-03-10T08:31:46.2064177Z Getting action download info
2026-03-10T08:31:46.6262063Z Download action repository 'actions/checkout@v4' (SHA:34e114876b0b11c390a56381ad16ebd13914f8d5)
2026-03-10T08:31:46.7795432Z Download action repository 'actions/setup-python@v5' (SHA:a26af69be951a213d495a4c3e4e4022e16d87065)
2026-03-10T08:31:46.9706542Z Complete job name: check-chapters
2026-03-10T08:31:47.0474995Z ##[group]Run actions/checkout@v4
2026-03-10T08:31:47.0476286Z with:
2026-03-10T08:31:47.0477137Z   repository: johannesleidel1987-ai/cdreader-checker
2026-03-10T08:31:47.0478283Z   token: ***
2026-03-10T08:31:47.0479003Z   ssh-strict: true
2026-03-10T08:31:47.0479730Z   ssh-user: git
2026-03-10T08:31:47.0480476Z   persist-credentials: true
2026-03-10T08:31:47.0481298Z   clean: true
2026-03-10T08:31:47.0482045Z   sparse-checkout-cone-mode: true
2026-03-10T08:31:47.0483054Z   fetch-depth: 1
2026-03-10T08:31:47.0483806Z   fetch-tags: false
2026-03-10T08:31:47.0484538Z   show-progress: true
2026-03-10T08:31:47.0485304Z   lfs: false
2026-03-10T08:31:47.0486004Z   submodules: false
2026-03-10T08:31:47.0486755Z   set-safe-directory: true
2026-03-10T08:31:47.0487813Z ##[endgroup]
2026-03-10T08:31:47.1651486Z Syncing repository: johannesleidel1987-ai/cdreader-checker
2026-03-10T08:31:47.1655525Z ##[group]Getting Git version info
2026-03-10T08:31:47.1657729Z Working directory is '/home/runner/work/cdreader-checker/cdreader-checker'
2026-03-10T08:31:47.1660448Z [command]/usr/bin/git version
2026-03-10T08:31:47.1711202Z git version 2.53.0
2026-03-10T08:31:47.1741565Z ##[endgroup]
2026-03-10T08:31:47.1759614Z Temporarily overriding HOME='/home/runner/work/_temp/b68c7720-af52-4e5e-9216-99b4d47c92fa' before making global git config changes
2026-03-10T08:31:47.1763249Z Adding repository directory to the temporary git global config as a safe directory
2026-03-10T08:31:47.1778655Z [command]/usr/bin/git config --global --add safe.directory /home/runner/work/cdreader-checker/cdreader-checker
2026-03-10T08:31:47.1825205Z Deleting the contents of '/home/runner/work/cdreader-checker/cdreader-checker'
2026-03-10T08:31:47.1829087Z ##[group]Initializing the repository
2026-03-10T08:31:47.1833983Z [command]/usr/bin/git init /home/runner/work/cdreader-checker/cdreader-checker
2026-03-10T08:31:47.1968880Z hint: Using 'master' as the name for the initial branch. This default branch name
2026-03-10T08:31:47.1971508Z hint: will change to "main" in Git 3.0. To configure the initial branch name
2026-03-10T08:31:47.1974584Z hint: to use in all of your new repositories, which will suppress this warning,
2026-03-10T08:31:47.1976513Z hint: call:
2026-03-10T08:31:47.1977648Z hint:
2026-03-10T08:31:47.1979078Z hint: 	git config --global init.defaultBranch <name>
2026-03-10T08:31:47.1980800Z hint:
2026-03-10T08:31:47.1982670Z hint: Names commonly chosen instead of 'master' are 'main', 'trunk' and
2026-03-10T08:31:47.1985137Z hint: 'development'. The just-created branch can be renamed via this command:
2026-03-10T08:31:47.1987086Z hint:
2026-03-10T08:31:47.1988252Z hint: 	git branch -m <name>
2026-03-10T08:31:47.1989572Z hint:
2026-03-10T08:31:47.1991178Z hint: Disable this message with "git config set advice.defaultBranchName false"
2026-03-10T08:31:47.1994465Z Initialized empty Git repository in /home/runner/work/cdreader-checker/cdreader-checker/.git/
2026-03-10T08:31:47.1999325Z [command]/usr/bin/git remote add origin https://github.com/johannesleidel1987-ai/cdreader-checker
2026-03-10T08:31:47.2028414Z ##[endgroup]
2026-03-10T08:31:47.2033061Z ##[group]Disabling automatic garbage collection
2026-03-10T08:31:47.2034744Z [command]/usr/bin/git config --local gc.auto 0
2026-03-10T08:31:47.2074301Z ##[endgroup]
2026-03-10T08:31:47.2076274Z ##[group]Setting up auth
2026-03-10T08:31:47.2080293Z [command]/usr/bin/git config --local --name-only --get-regexp core\.sshCommand
2026-03-10T08:31:47.2115002Z [command]/usr/bin/git submodule foreach --recursive sh -c "git config --local --name-only --get-regexp 'core\.sshCommand' && git config --local --unset-all 'core.sshCommand' || :"
2026-03-10T08:31:47.2495898Z [command]/usr/bin/git config --local --name-only --get-regexp http\.https\:\/\/github\.com\/\.extraheader
2026-03-10T08:31:47.2532366Z [command]/usr/bin/git submodule foreach --recursive sh -c "git config --local --name-only --get-regexp 'http\.https\:\/\/github\.com\/\.extraheader' && git config --local --unset-all 'http.https://github.com/.extraheader' || :"
2026-03-10T08:31:47.2773488Z [command]/usr/bin/git config --local --name-only --get-regexp ^includeIf\.gitdir:
2026-03-10T08:31:47.2806769Z [command]/usr/bin/git submodule foreach --recursive git config --local --show-origin --name-only --get-regexp remote.origin.url
2026-03-10T08:31:47.3053490Z [command]/usr/bin/git config --local http.https://github.com/.extraheader AUTHORIZATION: basic ***
2026-03-10T08:31:47.3089764Z ##[endgroup]
2026-03-10T08:31:47.3091234Z ##[group]Fetching the repository
2026-03-10T08:31:47.3098623Z [command]/usr/bin/git -c protocol.version=2 fetch --no-tags --prune --no-recurse-submodules --depth=1 origin +c1ecb300800892d09abe33d3aca980890f3aa801:refs/remotes/origin/main
2026-03-10T08:31:47.5596961Z From https://github.com/johannesleidel1987-ai/cdreader-checker
2026-03-10T08:31:47.5598347Z  * [new ref]         c1ecb300800892d09abe33d3aca980890f3aa801 -> origin/main
2026-03-10T08:31:47.5632782Z ##[endgroup]
2026-03-10T08:31:47.5634153Z ##[group]Determining the checkout info
2026-03-10T08:31:47.5635608Z ##[endgroup]
2026-03-10T08:31:47.5640276Z [command]/usr/bin/git sparse-checkout disable
2026-03-10T08:31:47.5687363Z [command]/usr/bin/git config --local --unset-all extensions.worktreeConfig
2026-03-10T08:31:47.5716499Z ##[group]Checking out the ref
2026-03-10T08:31:47.5720812Z [command]/usr/bin/git checkout --progress --force -B main refs/remotes/origin/main
2026-03-10T08:31:47.5769693Z Switched to a new branch 'main'
2026-03-10T08:31:47.5773410Z branch 'main' set up to track 'origin/main'.
2026-03-10T08:31:47.5780386Z ##[endgroup]
2026-03-10T08:31:47.5815432Z [command]/usr/bin/git log -1 --format=%H
2026-03-10T08:31:47.5838452Z c1ecb300800892d09abe33d3aca980890f3aa801
2026-03-10T08:31:47.6178253Z ##[group]Run actions/setup-python@v5
2026-03-10T08:31:47.6179090Z with:
2026-03-10T08:31:47.6179688Z   python-version: 3.11
2026-03-10T08:31:47.6180361Z   check-latest: false
2026-03-10T08:31:47.6181208Z   token: ***
2026-03-10T08:31:47.6181905Z   update-environment: true
2026-03-10T08:31:47.6182954Z   allow-prereleases: false
2026-03-10T08:31:47.6183935Z   freethreaded: false
2026-03-10T08:31:47.6184586Z ##[endgroup]
2026-03-10T08:31:47.7904404Z ##[group]Installed versions
2026-03-10T08:31:47.8012270Z Successfully set up CPython (3.11.14)
2026-03-10T08:31:47.8015512Z ##[endgroup]
2026-03-10T08:31:47.8163431Z ##[group]Run pip install requests
2026-03-10T08:31:47.8164243Z [36;1mpip install requests[0m
2026-03-10T08:31:47.8225550Z shell: /usr/bin/bash -e {0}
2026-03-10T08:31:47.8226331Z env:
2026-03-10T08:31:47.8227045Z   pythonLocation: /opt/hostedtoolcache/Python/3.11.14/x64
2026-03-10T08:31:47.8228061Z   PKG_CONFIG_PATH: /opt/hostedtoolcache/Python/3.11.14/x64/lib/pkgconfig
2026-03-10T08:31:47.8229062Z   Python_ROOT_DIR: /opt/hostedtoolcache/Python/3.11.14/x64
2026-03-10T08:31:47.8229975Z   Python2_ROOT_DIR: /opt/hostedtoolcache/Python/3.11.14/x64
2026-03-10T08:31:47.8230897Z   Python3_ROOT_DIR: /opt/hostedtoolcache/Python/3.11.14/x64
2026-03-10T08:31:47.8231843Z   LD_LIBRARY_PATH: /opt/hostedtoolcache/Python/3.11.14/x64/lib
2026-03-10T08:31:47.8232855Z ##[endgroup]
2026-03-10T08:31:50.3943774Z Collecting requests
2026-03-10T08:31:50.4524713Z   Downloading requests-2.32.5-py3-none-any.whl.metadata (4.9 kB)
2026-03-10T08:31:50.5650680Z Collecting charset_normalizer<4,>=2 (from requests)
2026-03-10T08:31:50.5691211Z   Downloading charset_normalizer-3.4.5-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl.metadata (39 kB)
2026-03-10T08:31:50.5976600Z Collecting idna<4,>=2.5 (from requests)
2026-03-10T08:31:50.6020890Z   Downloading idna-3.11-py3-none-any.whl.metadata (8.4 kB)
2026-03-10T08:31:50.6250096Z Collecting urllib3<3,>=1.21.1 (from requests)
2026-03-10T08:31:50.6289326Z   Downloading urllib3-2.6.3-py3-none-any.whl.metadata (6.9 kB)
2026-03-10T08:31:50.6474244Z Collecting certifi>=2017.4.17 (from requests)
2026-03-10T08:31:50.6513157Z   Downloading certifi-2026.2.25-py3-none-any.whl.metadata (2.5 kB)
2026-03-10T08:31:50.6592672Z Downloading requests-2.32.5-py3-none-any.whl (64 kB)
2026-03-10T08:31:50.6662887Z Downloading charset_normalizer-3.4.5-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl (193 kB)
2026-03-10T08:31:50.6751229Z Downloading idna-3.11-py3-none-any.whl (71 kB)
2026-03-10T08:31:50.6808692Z Downloading urllib3-2.6.3-py3-none-any.whl (131 kB)
2026-03-10T08:31:50.6867568Z Downloading certifi-2026.2.25-py3-none-any.whl (153 kB)
2026-03-10T08:31:50.7438016Z Installing collected packages: urllib3, idna, charset_normalizer, certifi, requests
2026-03-10T08:31:50.9790685Z 
2026-03-10T08:31:50.9801653Z Successfully installed certifi-2026.2.25 charset_normalizer-3.4.5 idna-3.11 requests-2.32.5 urllib3-2.6.3
2026-03-10T08:31:51.1406647Z ##[group]Run python checker.py
2026-03-10T08:31:51.1407059Z [36;1mpython checker.py[0m
2026-03-10T08:31:51.1460042Z shell: /usr/bin/bash -e {0}
2026-03-10T08:31:51.1460383Z env:
2026-03-10T08:31:51.1460739Z   pythonLocation: /opt/hostedtoolcache/Python/3.11.14/x64
2026-03-10T08:31:51.1461290Z   PKG_CONFIG_PATH: /opt/hostedtoolcache/Python/3.11.14/x64/lib/pkgconfig
2026-03-10T08:31:51.1461855Z   Python_ROOT_DIR: /opt/hostedtoolcache/Python/3.11.14/x64
2026-03-10T08:31:51.1462337Z   Python2_ROOT_DIR: /opt/hostedtoolcache/Python/3.11.14/x64
2026-03-10T08:31:51.1463127Z   Python3_ROOT_DIR: /opt/hostedtoolcache/Python/3.11.14/x64
2026-03-10T08:31:51.1463637Z   LD_LIBRARY_PATH: /opt/hostedtoolcache/Python/3.11.14/x64/lib
2026-03-10T08:31:51.1464345Z   CDREADER_EMAIL: ***
2026-03-10T08:31:51.1464717Z   CDREADER_PASSWORD: ***
2026-03-10T08:31:51.1465175Z   TELEGRAM_BOT_TOKEN: ***
2026-03-10T08:31:51.1465526Z   TELEGRAM_CHAT_ID: ***
2026-03-10T08:31:51.1465942Z   GEMINI_API_KEY: ***
2026-03-10T08:31:51.1466377Z   GEMINI_API_KEY_2: ***
2026-03-10T08:31:51.1466781Z   GEMINI_API_KEY_3: ***
2026-03-10T08:31:51.1467177Z   GEMINI_API_KEY_4: ***
2026-03-10T08:31:51.1467567Z   GEMINI_API_KEY_5: ***
2026-03-10T08:31:51.1467951Z   GEMINI_API_KEY_6: ***
2026-03-10T08:31:51.1468347Z   GEMINI_API_KEY_7: ***
2026-03-10T08:31:51.1468739Z   GEMINI_API_KEY_8: ***
2026-03-10T08:31:51.1469381Z   GEMINI_API_KEY_9: ***
2026-03-10T08:31:51.1469784Z   GEMINI_API_KEY_10: ***
2026-03-10T08:31:51.1470212Z   GEMINI_API_KEY_11: ***
2026-03-10T08:31:51.1470602Z   GEMINI_API_KEY_12: ***
2026-03-10T08:31:51.1471021Z   GEMINI_API_KEY_13: ***
2026-03-10T08:31:51.1471442Z   GEMINI_API_KEY_14: ***
2026-03-10T08:31:51.1471841Z   GEMINI_API_KEY_15: ***
2026-03-10T08:31:51.1472250Z   GEMINI_API_KEY_16: ***
2026-03-10T08:31:51.1472794Z   GEMINI_API_KEY_17: ***
2026-03-10T08:31:51.1473205Z   GEMINI_API_KEY_18: ***
2026-03-10T08:31:51.1473615Z   GEMINI_API_KEY_19: ***
2026-03-10T08:31:51.1474038Z   GEMINI_API_KEY_20: ***
2026-03-10T08:31:51.1474454Z   GEMINI_API_KEY_21: ***
2026-03-10T08:31:51.1474869Z   GEMINI_API_KEY_22: ***
2026-03-10T08:31:51.1475275Z   GEMINI_API_KEY_23: ***
2026-03-10T08:31:51.1475655Z   GEMINI_API_KEY_24: ***
2026-03-10T08:31:51.1476019Z   GEMINI_API_KEY_25: ***
2026-03-10T08:31:51.1476404Z   GEMINI_API_KEY_26: ***
2026-03-10T08:31:51.1476776Z   GEMINI_API_KEY_27: ***
2026-03-10T08:31:51.1477155Z   GEMINI_API_KEY_28: ***
2026-03-10T08:31:51.1477445Z   TEST_MODE: false
2026-03-10T08:31:51.1477724Z   OVERRIDE_CHAPTER_ID: 
2026-03-10T08:31:51.1478009Z   OVERRIDE_BOOK_ID: 
2026-03-10T08:31:51.1478297Z ##[endgroup]
2026-03-10T08:31:51.2903502Z [08:31:51] Logging in...
2026-03-10T08:31:52.0346875Z [08:31:52] Logged in.
2026-03-10T08:31:52.0356310Z [08:31:52] Fetching book list...
2026-03-10T08:31:53.2786831Z [08:31:53]   Page 1: 18 book(s).
2026-03-10T08:31:53.2787621Z [08:31:53] Total books: 18
2026-03-10T08:31:53.2800540Z [08:31:53] Checking for already active chapter across all books...
2026-03-10T08:31:53.2801669Z [08:31:53]   Checking Task Center for active chapter...
2026-03-10T08:31:57.2382318Z [08:31:57]   Task Center: 10 task(s) found
2026-03-10T08:31:57.2384754Z [08:31:57]   Task fields: ['id', 'taskType', 'taskTitle', 'taskContent', 'taskUrl', 'taskMenu', 'target', 'status', 'createTime', 'optUsers', 'optPhones', 'chapterType', 'chapterId', 'authorId', 'toLanguage', 'finishTime', 'bookId']
2026-03-10T08:31:57.2392933Z [08:31:57]   Task: {'id': 795154, 'taskType': '低分重校章节', 'taskTitle': '低分重校章节', 'taskContent': "Love's Prescription: The Small-Town Girl Is An Extraordinary Healer|Das Mädchen aus der Kleinstadt wird zur außergewöhnlichen Heilerin|Chapter 80 We Owe Verena", 'taskUrl': 'ProofreadingForeignersList|1145733|8506', 'taskMenu': '', 'target': 0, 'status': 1, 'createTime': '2026-03-10T15:36:36.03', 'optUsers': 'Dennis Woocker(820251)', 'optPhones': '', 'chapterType': 6, 'chapterId': 1145733, 'authorId': 820251, 'toLanguage': 412, 'finishTime': '2026-03-10T15:42:03.158', 'bookId': 0}
2026-03-10T08:31:57.2402983Z [08:31:57]   Task: {'id': 795139, 'taskType': '低分重校章节', 'taskTitle': '低分重校章节', 'taskContent': "Love's Sweetest Surprise: From Brokenhearted To Billionaire's Wife|Vom Liebeskummer zur Milliardärsgattin|Chapter 19 This Wasn't A Fair Fight", 'taskUrl': 'ProofreadingForeignersList|1145806|8574', 'taskMenu': '', 'target': 0, 'status': 1, 'createTime': '2026-03-10T15:21:54.71', 'optUsers': 'Dennis Woocker(820251)', 'optPhones': '', 'chapterType': 6, 'chapterId': 1145806, 'authorId': 820251, 'toLanguage': 412, 'finishTime': '2026-03-10T15:22:52.433', 'bookId': 0}
2026-03-10T08:31:57.2410668Z [08:31:57]   Task: {'id': 795138, 'taskType': '一校抽查未修改章节', 'taskTitle': '一校抽查未修改章节', 'taskContent': "Love's Prescription: The Small-Town Girl Is An Extraordinary Healer|Das Mädchen aus der Kleinstadt wird zur außergewöhnlichen Heilerin|Chapter 80 We Owe Verena", 'taskUrl': 'ProofreadingForeignersList|1145733|8506', 'taskMenu': '', 'target': 0, 'status': 1, 'createTime': '2026-03-10T15:21:53.766', 'optUsers': 'Dennis Woocker(820251)', 'optPhones': '', 'chapterType': 4, 'chapterId': 1145733, 'authorId': 820251, 'toLanguage': 412, 'finishTime': '2026-03-10T15:39:14.862', 'bookId': 0}
2026-03-10T08:31:57.2415920Z [08:31:57]   Task: {'id': 795112, 'taskType': '领稿未一校完成章节', 'taskTitle': '领稿未一校完成章节', 'taskContent': "Love's Sweetest Surprise: From Brokenhearted To Billionaire's Wife|Vom Liebeskummer zur Milliardärsgattin|Chapter 22 Make Her Intentions Clear", 'taskUrl': 'ProofreadingForeignersList|1145824|8574', 'taskMenu': '', 'target': 0, 'status': 1, 'createTime': '2026-03-10T15:07:18.787', 'optUsers': 'Dennis Woocker(820251)', 'optPhones': '', 'chapterType': 2, 'chapterId': 1145824, 'authorId': 820251, 'toLanguage': 412, 'finishTime': '2026-03-10T15:19:59.724', 'bookId': 0}
2026-03-10T08:31:57.2420251Z [08:31:57]   Task: {'id': 795002, 'taskType': '领稿未一校完成章节', 'taskTitle': '领稿未一校完成章节', 'taskContent': "Love's Sweetest Surprise: From Brokenhearted To Billionaire's Wife|Vom Liebeskummer zur Milliardärsgattin|Chapter 20 Learn About Her Hospital Visit", 'taskUrl': 'ProofreadingForeignersList|1145807|8574', 'taskMenu': '', 'target': 0, 'status': 1, 'createTime': '2026-03-10T14:34:25.907', 'optUsers': 'Dennis Woocker(820251)', 'optPhones': '', 'chapterType': 2, 'chapterId': 1145807, 'authorId': 820251, 'toLanguage': 412, 'finishTime': '2026-03-10T14:35:07.98', 'bookId': 0}
2026-03-10T08:31:57.2424339Z [08:31:57]   Task: {'id': 794909, 'taskType': '领稿未一校完成章节', 'taskTitle': '领稿未一校完成章节', 'taskContent': "Love's Sweetest Surprise: From Brokenhearted To Billionaire's Wife|Vom Liebeskummer zur Milliardärsgattin|Chapter 17 Strike First", 'taskUrl': 'ProofreadingForeignersList|1145804|8574', 'taskMenu': '', 'target': 0, 'status': 1, 'createTime': '2026-03-10T13:05:53.99', 'optUsers': 'Dennis Woocker(820251)', 'optPhones': '', 'chapterType': 2, 'chapterId': 1145804, 'authorId': 820251, 'toLanguage': 412, 'finishTime': '2026-03-10T13:06:58.813', 'bookId': 0}
2026-03-10T08:31:57.2428654Z [08:31:57]   Task: {'id': 794834, 'taskType': '领稿未一校完成章节', 'taskTitle': '领稿未一校完成章节', 'taskContent': "Love's Sweetest Surprise: From Brokenhearted To Billionaire's Wife|Vom Liebeskummer zur Milliardärsgattin|Chapter 14 Her Weak Point", 'taskUrl': 'ProofreadingForeignersList|1145801|8574', 'taskMenu': '', 'target': 0, 'status': 1, 'createTime': '2026-03-10T11:33:05.767', 'optUsers': 'Dennis Woocker(820251)', 'optPhones': '', 'chapterType': 2, 'chapterId': 1145801, 'authorId': 820251, 'toLanguage': 412, 'finishTime': '2026-03-10T11:48:23.768', 'bookId': 0}
2026-03-10T08:31:57.2432906Z [08:31:57]   Task: {'id': 794820, 'taskType': '领稿未一校完成章节', 'taskTitle': '领稿未一校完成章节', 'taskContent': "Love's Sweetest Surprise: From Brokenhearted To Billionaire's Wife|Vom Liebeskummer zur Milliardärsgattin|Chapter 13 His Disgusting Pretense", 'taskUrl': 'ProofreadingForeignersList|1145800|8574', 'taskMenu': '', 'target': 0, 'status': 1, 'createTime': '2026-03-10T11:07:03.857', 'optUsers': 'Dennis Woocker(820251)', 'optPhones': '', 'chapterType': 2, 'chapterId': 1145800, 'authorId': 820251, 'toLanguage': 412, 'finishTime': '2026-03-10T11:20:11.66', 'bookId': 0}
2026-03-10T08:31:57.2436885Z [08:31:57]   Task: {'id': 794809, 'taskType': '领稿未一校完成章节', 'taskTitle': '领稿未一校完成章节', 'taskContent': "Love's Sweetest Surprise: From Brokenhearted To Billionaire's Wife|Vom Liebeskummer zur Milliardärsgattin|Chapter 12 Who Did He Think He Was?", 'taskUrl': 'ProofreadingForeignersList|1145799|8574', 'taskMenu': '', 'target': 0, 'status': 1, 'createTime': '2026-03-10T10:36:11.428', 'optUsers': 'Dennis Woocker(820251)', 'optPhones': '', 'chapterType': 2, 'chapterId': 1145799, 'authorId': 820251, 'toLanguage': 412, 'finishTime': '2026-03-10T10:48:21.996', 'bookId': 0}
2026-03-10T08:31:57.2441293Z [08:31:57]   Task: {'id': 794781, 'taskType': '领稿未一校完成章节', 'taskTitle': '领稿未一校完成章节', 'taskContent': "Love's Sweetest Surprise: From Brokenhearted To Billionaire's Wife|Vom Liebeskummer zur Milliardärsgattin|Chapter 11 She Had Been Rear-Ended", 'taskUrl': 'ProofreadingForeignersList|1145798|8574', 'taskMenu': '', 'target': 0, 'status': 1, 'createTime': '2026-03-10T10:03:28.595', 'optUsers': 'Dennis Woocker(820251)', 'optPhones': '', 'chapterType': 2, 'chapterId': 1145798, 'authorId': 820251, 'toLanguage': 412, 'finishTime': '2026-03-10T10:18:40.354', 'bookId': 0}
2026-03-10T08:31:57.2443496Z [08:31:57]   Evaluating task status=1 finishTime=2026-03-10T15:42:03.158
2026-03-10T08:31:57.2444092Z [08:31:57]   Skipping task — finishTime is set (2026-03-10T15:42:03.158)
2026-03-10T08:31:57.2444752Z [08:31:57]   Evaluating task status=1 finishTime=2026-03-10T15:22:52.433
2026-03-10T08:31:57.2445307Z [08:31:57]   Skipping task — finishTime is set (2026-03-10T15:22:52.433)
2026-03-10T08:31:57.2445794Z [08:31:57]   Evaluating task status=1 finishTime=2026-03-10T15:39:14.862
2026-03-10T08:31:57.2446339Z [08:31:57]   Skipping task — finishTime is set (2026-03-10T15:39:14.862)
2026-03-10T08:31:57.2446824Z [08:31:57]   Evaluating task status=1 finishTime=2026-03-10T15:19:59.724
2026-03-10T08:31:57.2447385Z [08:31:57]   Skipping task — finishTime is set (2026-03-10T15:19:59.724)
2026-03-10T08:31:57.2447872Z [08:31:57]   Evaluating task status=1 finishTime=2026-03-10T14:35:07.98
2026-03-10T08:31:57.2448432Z [08:31:57]   Skipping task — finishTime is set (2026-03-10T14:35:07.98)
2026-03-10T08:31:57.2448936Z [08:31:57]   Evaluating task status=1 finishTime=2026-03-10T13:06:58.813
2026-03-10T08:31:57.2449493Z [08:31:57]   Skipping task — finishTime is set (2026-03-10T13:06:58.813)
2026-03-10T08:31:57.2449995Z [08:31:57]   Evaluating task status=1 finishTime=2026-03-10T11:48:23.768
2026-03-10T08:31:57.2450537Z [08:31:57]   Skipping task — finishTime is set (2026-03-10T11:48:23.768)
2026-03-10T08:31:57.2451029Z [08:31:57]   Evaluating task status=1 finishTime=2026-03-10T11:20:11.66
2026-03-10T08:31:57.2451573Z [08:31:57]   Skipping task — finishTime is set (2026-03-10T11:20:11.66)
2026-03-10T08:31:57.2452063Z [08:31:57]   Evaluating task status=1 finishTime=2026-03-10T10:48:21.996
2026-03-10T08:31:57.2452742Z [08:31:57]   Skipping task — finishTime is set (2026-03-10T10:48:21.996)
2026-03-10T08:31:57.2453252Z [08:31:57]   Evaluating task status=1 finishTime=2026-03-10T10:18:40.354
2026-03-10T08:31:57.2453830Z [08:31:57]   Skipping task — finishTime is set (2026-03-10T10:18:40.354)
2026-03-10T08:31:57.2454293Z [08:31:57]   No active chapter found in Task Center.
2026-03-10T08:31:57.2454838Z [08:31:57] Checking: Einst Verstoßen, Jetzt Begehrt (ID: 8575)
2026-03-10T08:31:57.9167463Z [08:31:57]     Chapter API: code=315, available=0
2026-03-10T08:31:57.9177912Z [08:31:57]   No available chapters.
2026-03-10T08:31:57.9179246Z [08:31:57] Checking: Vom Liebeskummer zur Milliardärsgattin (ID: 8574)
2026-03-10T08:31:58.7532668Z [08:31:58]     Chapter API: code=315, available=1
2026-03-10T08:31:58.7542780Z [08:31:58]   1 chapter(s) available!
2026-03-10T08:31:59.7152151Z [08:31:59]   ✅ Claimed: Chapter 23 Not Now
2026-03-10T08:31:59.7153317Z [08:31:59]   Claim response data: None → proc_id=None
2026-03-10T08:31:59.7153814Z [08:31:59] 
2026-03-10T08:31:59.7154834Z ── Processing: Vom Liebeskummer zur Milliardärsgattin / Chapter 23 Not Now ──
2026-03-10T08:31:59.7155625Z [08:31:59]   proc_id resolved: 1146030 (claim_response=None, ch_id=1146030)
2026-03-10T08:31:59.7156253Z [08:31:59]   Starting chapter 1146030...
2026-03-10T08:32:00.3742265Z [08:32:00]   Start OK (message=SaveSuccess)
2026-03-10T08:32:02.3758506Z [08:32:02]   Fetching rows for chapter 1146030...
2026-03-10T08:32:04.0985428Z [08:32:04]   Row fields available: ['id', 'objectChapterId', 'chapterConetnt', 'eContent', 'eeContent', 'peContent', 'modifChapterContent', 'machineChapterContent', 'referenceContent', 'languageContent', 'wordCorrection', 'qualityFeedBack', 'messageContext', 'status', 'sort', 'passStatus', 'isLock', 'foreignPassStatus', 'isNoSelf', 'inspectorsPassStatus', 'proofreadPassStatus', 'checkForeignPassStatus', 'delStatus', 'materialId', 'materialType', 'materialUrl', 'subtitlesTime', 'contentCode', 'glossaryList']
2026-03-10T08:32:04.0988539Z [08:32:04]   First row sample: sort=0 | eContent='' | chapterConetnt='Chapter 23 Not Now'
2026-03-10T08:32:04.0990976Z [08:32:04]   [DIAG] sort=1 | chapterConetnt="Bethany's words struck the lavish room with the force of a l" | machineChapterContent='Bettinas Worte trafen den prächtigen Raum wie ein Blitzschla' | modifChapterContent='Bettinas Worte trafen den prächtigen Raum wie ein Blitzschla' | languageContent='这五个字如同惊雷，在奢华的客厅里炸开，震得霍逸辰耳膜嗡嗡作响。' | peContent='Bettinas Worte trafen den prächtigen Raum wie ein Blitzschla'
2026-03-10T08:32:04.0996278Z [08:32:04]   [DIAG] sort=2 | chapterConetnt='Brodie gaped at her, stunned by the transformation in her ey' | machineChapterContent='Bruno starrte sie an, fassungslos über die Veränderung in ih' | modifChapterContent='Bruno starrte sie an, fassungslos über die Veränderung in ih' | languageContent='他死死盯着宋婠，那双总是带着讨好的眼眸，此刻只剩下他从未见过的冷漠。' | peContent='Bruno starrte sie an, fassungslos über die Veränderung in ih'
2026-03-10T08:32:04.1001458Z [08:32:04]   [DIAG] sort=3 | chapterConetnt='The warmth and longing he once saw there had turned to a col' | machineChapterContent='Die Wärme und Sehnsucht, die er einst dort gesehen hatte, wa' | modifChapterContent='Die Wärme und Sehnsucht, die er einst dort gesehen hatte, wa' | languageContent='' | peContent='Die Wärme und Sehnsucht, die er einst dort gesehen hatte, wa'
2026-03-10T08:32:04.1004281Z [08:32:04]   Fetched 109 rows.
2026-03-10T08:32:04.1004974Z [08:32:04]   Fetching glossary for book 8574...
2026-03-10T08:32:05.0990512Z [08:32:05]   Glossary term fields: ['id', 'objectBookId', 'cnValue', 'dictionaryKey', 'dictionaryValue', 'enSurname', 'dictionarySurname', 'dictionaryRemark', 'objectStatus', 'objectStatusName', 'interpreterId', 'interpreterName', 'email', 'relateContent', 'dicValueBackUp', 'dicRemarkBackUp', 'delFlag', 'delFlagName', 'createTime', 'orderByKey', 'orderByValue', 'orderByRemark', 'orderByRemarkBackUp', 'orderByRelateContent', 'orderByName', 'occurcount', 'firstContentTexts', 'isEdit']
2026-03-10T08:32:05.0995853Z [08:32:05]   Glossary page 1: 100 terms.
2026-03-10T08:32:06.1112734Z [08:32:06]   Glossary page 2: 100 terms.
2026-03-10T08:32:06.9295419Z [08:32:06]   Glossary page 3: 50 terms.
2026-03-10T08:32:06.9295983Z [08:32:06]   Total glossary terms: 250
2026-03-10T08:32:06.9306102Z [08:32:06]   Rephrasing 109 rows with Gemini...
2026-03-10T08:32:06.9315240Z [08:32:06]   Input data: 109 rows, 109 with non-empty content
2026-03-10T08:32:06.9316948Z [08:32:06]   Field presence: chapterConetnt=True, eContent=False, eeContent=False, modifChapterContent=True, machineChapterContent=True, languageContent=True, peContent=True, referenceContent=False
2026-03-10T08:32:06.9319001Z [08:32:06]   Splitting 108 rows into 3 batches of ~40...
2026-03-10T08:32:06.9320137Z [08:32:06]   Using 28 Gemini key(s) (keys 1-28) with automatic rotation on 429.
2026-03-10T08:32:06.9321196Z [08:32:06]   Sending batch 1/3 (40 rows) via Gemini...
2026-03-10T08:32:06.9330455Z [08:32:06]   Glossary for batch 1: 9 relevant terms (of 250 total)
2026-03-10T08:32:36.8739946Z [08:32:36]   Batch 1/3: 40 rows from Gemini.
2026-03-10T08:32:41.8757150Z [08:32:41]   Sending batch 2/3 (40 rows) via Gemini...
2026-03-10T08:32:41.8777588Z [08:32:41]   Glossary for batch 2: 10 relevant terms (of 250 total)
2026-03-10T08:33:24.0843319Z [08:33:24]   Batch 2/3: 40 rows from Gemini.
2026-03-10T08:33:29.0858664Z [08:33:29]   Sending batch 3/3 (28 rows) via Gemini...
2026-03-10T08:33:29.0871584Z [08:33:29]   Glossary for batch 3: 10 relevant terms (of 250 total)
2026-03-10T08:33:29.2558937Z [08:33:29]   📵 Key daily quota (RPD) exhausted [you exceeded your current quota, please check your plan and billing details. for], 27 key(s) left...
2026-03-10T08:33:55.7814307Z [08:33:55]   Batch 3/3: 28 rows from Gemini.
2026-03-10T08:33:55.7823467Z [08:33:55]   ⚠️  Bleed guard: sort=99 inflated (9w vs 5w input) — restored from MT
2026-03-10T08:33:55.7824716Z [08:33:55]   💬 Bleed guard: restored 1 inflated row(s) from MT (will retry).
2026-03-10T08:33:55.7827960Z [08:33:55]   Total rows rephrased: 108
2026-03-10T08:33:55.7876021Z [08:33:55]   ⚠️  BGS confusion: sort=73 restored from 'Er wollte zusehen, wie sie sich ohne die Unterstützung der Familie Wilson durchschlug.' to MT 'Er wollte zusehen, wie sie ohne die Unterstützung der Famili'
2026-03-10T08:33:55.7890069Z [08:33:55]   💬 BGS confusion guard: restored 1 row(s) from MT.
2026-03-10T08:33:55.7892157Z [08:33:55]   ⚠️  Quote opener: sort=64 EN starts quote -- injected „: '„Ich kann das selbst erledigen!“'
2026-03-10T08:33:55.7893831Z [08:33:55]   💬 EN-source quote opener: fixed 1 row(s).
2026-03-10T08:33:55.7911567Z [08:33:55]   💬 Post-processing: enforced quote structure in 1 row(s) (Pass QE).
2026-03-10T08:33:55.7927928Z [08:33:55] ❌ Unhandled exception in pipeline: sub() missing 2 required positional arguments: 'repl' and 'string'
2026-03-10T08:33:55.7947772Z Traceback (most recent call last):
2026-03-10T08:33:55.7971196Z   File "/home/runner/work/cdreader-checker/cdreader-checker/checker.py", line 2592, in run
2026-03-10T08:33:55.7972199Z     _run_inner(token)
2026-03-10T08:33:55.7973453Z   File "/home/runner/work/cdreader-checker/cdreader-checker/checker.py", line 2786, in _run_inner
2026-03-10T08:33:55.7974600Z     rephrased = rephrase_with_gemini(rows, glossary, book_name)
2026-03-10T08:33:55.7975393Z                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-03-10T08:33:55.7976557Z   File "/home/runner/work/cdreader-checker/cdreader-checker/checker.py", line 2338, in rephrase_with_gemini
2026-03-10T08:33:55.7977804Z     _post_process(sorted_rows, input_data, glossary_terms)
2026-03-10T08:33:55.7979013Z   File "/home/runner/work/cdreader-checker/cdreader-checker/checker.py", line 1692, in _post_process
2026-03-10T08:33:55.7980050Z     c_e = _re.sub(
2026-03-10T08:33:55.7980527Z           ^^^^^^^^
2026-03-10T08:33:55.7981255Z TypeError: sub() missing 2 required positional arguments: 'repl' and 'string'
2026-03-10T08:33:56.2409972Z [08:33:56] Telegram sent.
2026-03-10T08:33:56.2663807Z Post job cleanup.
2026-03-10T08:33:56.4389487Z Post job cleanup.
2026-03-10T08:33:56.5432068Z [command]/usr/bin/git version
2026-03-10T08:33:56.5474636Z git version 2.53.0
2026-03-10T08:33:56.5523640Z Temporarily overriding HOME='/home/runner/work/_temp/9a277d22-75e4-494d-a754-6ed79e74ef77' before making global git config changes
2026-03-10T08:33:56.5525243Z Adding repository directory to the temporary git global config as a safe directory
2026-03-10T08:33:56.5531333Z [command]/usr/bin/git config --global --add safe.directory /home/runner/work/cdreader-checker/cdreader-checker
2026-03-10T08:33:56.5577138Z [command]/usr/bin/git config --local --name-only --get-regexp core\.sshCommand
2026-03-10T08:33:56.5614714Z [command]/usr/bin/git submodule foreach --recursive sh -c "git config --local --name-only --get-regexp 'core\.sshCommand' && git config --local --unset-all 'core.sshCommand' || :"
2026-03-10T08:33:56.5882245Z [command]/usr/bin/git config --local --name-only --get-regexp http\.https\:\/\/github\.com\/\.extraheader
2026-03-10T08:33:56.5910192Z http.https://github.com/.extraheader
2026-03-10T08:33:56.5923583Z [command]/usr/bin/git config --local --unset-all http.https://github.com/.extraheader
2026-03-10T08:33:56.5960201Z [command]/usr/bin/git submodule foreach --recursive sh -c "git config --local --name-only --get-regexp 'http\.https\:\/\/github\.com\/\.extraheader' && git config --local --unset-all 'http.https://github.com/.extraheader' || :"
2026-03-10T08:33:56.6216760Z [command]/usr/bin/git config --local --name-only --get-regexp ^includeIf\.gitdir:
2026-03-10T08:33:56.6254380Z [command]/usr/bin/git submodule foreach --recursive git config --local --show-origin --name-only --get-regexp remote.origin.url
2026-03-10T08:33:56.6626179Z Cleaning up orphan processes
