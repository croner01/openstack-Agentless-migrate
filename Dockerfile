FROM python:3.10-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Shanghai

# 数据面的 rbd CLI 必须与 Ceph 集群同代。源/目标集群都是 Nautilus 14.2.22，
# 而 bookworm 自带的 ceph-common 是 16.2 Pacific：Pacific 客户端解析 Nautilus
# 的 OSDMap 增量时会命中
#   ./src/osd/OSDMap.cc: FAILED ceph_assert(q != removed_snaps_queue.end())
# 直接 SIGABRT，表现为增量迁移时 export-diff 与 import-diff 在同一秒一起崩溃。
#
# Nautilus 只发布了 bionic 构建，且 bookworm 已没有 libssl1.1/libtinfo5/
# libncurses5，所以这里按校验和固定版本下载 deb，解包后只取 rbd 与所需 .so。
# 不能把 deb 直接解到 /：bionic 包里的 ./lib/ 会顶掉 bookworm 的
# /lib -> usr/lib 软链，镜像内所有动态链接程序都会找不到 loader。
#
# 镜像同时支持 amd64 与 arm64：bionic/deb11 对两种架构都出过包，按 buildx 注入
# 的 TARGETARCH 选择对应包名与 SHA256（下表全部取自官方 Packages 索引），
# 两种架构的多架构库目录名（x86_64-linux-gnu / aarch64-linux-gnu）也在此决定。
# 内网/镜像站可用 --build-arg CEPH_DEB_REPO=... 覆盖。
ARG TARGETARCH
ARG CEPH_DEB_REPO=https://mirrors.tuna.tsinghua.edu.cn/ceph/debian-nautilus
ARG DEBIAN_DEB_REPO=https://deb.debian.org/debian

RUN set -eux; \
    arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    ceph_pool="$CEPH_DEB_REPO/pool/main/c/ceph"; \
    deb_pool="$DEBIAN_DEB_REPO/pool/main"; \
    case "$arch" in \
        amd64) \
            multiarch=x86_64-linux-gnu; \
            specs=" \
                $ceph_pool/ceph-common_14.2.22-1bionic_amd64.deb|e07c3dd1be1b460dd566dc718227f6e5362d268acae7b2a560068553a096ee01 \
                $ceph_pool/librados2_14.2.22-1bionic_amd64.deb|efc4bf56549e9795f5b0393c4d83228f2360bed1dc645ecd0649e3e7201967d3 \
                $ceph_pool/librbd1_14.2.22-1bionic_amd64.deb|2810839eb498be83a8ac81ead695e79c6b17208f1c405dc52dbd38a7b40ae758 \
                $deb_pool/o/openssl/libssl1.1_1.1.1w-0+deb11u1_amd64.deb|aadf8b4b197335645b230c2839b4517aa444fd2e8f434e5438c48a18857988f7 \
                $deb_pool/n/ncurses/libtinfo5_6.2+20201114-2+deb11u2_amd64.deb|69e131ce3f790a892ca1b0ae3bfad8659daa2051495397eee1b627d9783a6797 \
                $deb_pool/n/ncurses/libncurses5_6.2+20201114-2+deb11u2_amd64.deb|29de4956385179ef68e6795230950aeb24d548c10e5f2cd78b90aa177d5cd81d" \
            ;; \
        arm64) \
            multiarch=aarch64-linux-gnu; \
            specs=" \
                $ceph_pool/ceph-common_14.2.22-1bionic_arm64.deb|f7890cd0e8d11bd30b015b46d46ed2625ef6f84442485fdc5f9f6a82b10ca3e0 \
                $ceph_pool/librados2_14.2.22-1bionic_arm64.deb|b48460ed6e8997b45c7546654217ad2741557722a4b831c1b22fbeaa8ebc50f0 \
                $ceph_pool/librbd1_14.2.22-1bionic_arm64.deb|e58ce0adbbf000ab05315fc8f71648b23f4baee1719b76342f44bc1ea1233dd7 \
                $deb_pool/o/openssl/libssl1.1_1.1.1w-0+deb11u1_arm64.deb|fe7a7d313c87e46e62e614a07137e4a476a79fc9e5aab7b23e8235211280fee3 \
                $deb_pool/n/ncurses/libtinfo5_6.2+20201114-2+deb11u2_arm64.deb|98a4b48202fa7f3f3191b5dc08bcee436b10ff7d01f9710a06309172b35677fb \
                $deb_pool/n/ncurses/libncurses5_6.2+20201114-2+deb11u2_arm64.deb|cebc7c767c8892bb49b82ff70b3ec3d13ffe3dc79ed0188c98b542fa5ea378c9" \
            ;; \
        *) \
            echo >&2 "不支持的架构 TARGETARCH=$arch（只提供 amd64/arm64）"; \
            exit 1 \
            ;; \
    esac; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates curl libibverbs1 librdmacm1 libnss3 libnspr4; \
    tmp="$(mktemp -d)"; cd "$tmp"; \
    for spec in $specs; do \
        url="${spec%%|*}"; sum="${spec##*|}"; \
        file="$(basename "$url")"; \
        curl -fsSL --retry 5 --retry-delay 2 -o "$file" "$url"; \
        echo "$sum  $file" >> SHA256SUMS; \
    done; \
    sha256sum -c SHA256SUMS; \
    for deb in ./*.deb; do dpkg-deb -x "$deb" root; done; \
    install -m 0755 root/usr/bin/rbd /usr/bin/rbd; \
    mkdir -p /usr/lib/ceph /usr/lib/"$multiarch" /etc/ld.so.conf.d; \
    cp -a root/usr/lib/ceph/. /usr/lib/ceph/; \
    for lib in root/usr/lib/*.so*; do \
        [ -e "$lib" ] || continue; \
        cp -a "$lib" /usr/lib/; \
    done; \
    for dir in "root/usr/lib/$multiarch" "root/lib/$multiarch"; do \
        [ -d "$dir" ] || continue; \
        cp -a "$dir"/*.so* /usr/lib/"$multiarch"/; \
    done; \
    echo /usr/lib/ceph > /etc/ld.so.conf.d/ceph.conf; \
    ldconfig; \
    rbd --version | grep -q '14\.2\.22'; \
    apt-get purge -y curl >/dev/null; \
    rm -rf "$tmp" /var/lib/apt/lists/*

WORKDIR /app

# 依赖下载源同样可用 --build-arg 覆盖：默认官方 PyPI，国内链路建议换成镜像站，
# 例如 --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple。
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple

COPY requirements.txt ./
RUN pip install --no-cache-dir --index-url "$PIP_INDEX_URL" \
        --timeout 60 --retries 5 -r requirements.txt

COPY . .

RUN mkdir -p /app/uploads

EXPOSE 19099

CMD ["python", "app.py"]
