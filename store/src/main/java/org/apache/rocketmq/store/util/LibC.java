package org.apache.rocketmq.store.util;

import com.sun.jna.*;

/**
 * 这里使用了JNA, 用于提供一组Java工具类用于在运行期动态访问系统本地共享类库而不需要编写任何Native/JNI代码.
 * <a href="https://github.com/java-native-access/jna">jna</a>
 */
public interface LibC extends Library {
    LibC INSTANCE = (LibC) Native.loadLibrary(Platform.isWindows() ? "msvcrt" : "c", LibC.class);

    int MADV_WILLNEED = 3;
    int MADV_DONTNEED = 4;

    int MCL_CURRENT = 1;
    int MCL_FUTURE = 2;
    int MCL_ONFAULT = 4;

    /* sync memory asynchronously */ int MS_ASYNC = 0x0001;
    /* invalidate mappings & caches */ int MS_INVALIDATE = 0x0002;
    /* synchronous memory sync */ int MS_SYNC = 0x0004;

    /**
     * 对标系统调用-mlock(), 它允许程序在物理内存上锁住它的部分或全部地址空间, 这将会阻止
     * 操作系统将这个内存页调度到交换空间(swap space). 锁定一个内存区间只需要简单将
     * 【指向内存区间开始的指针】+【内存区间长度】作为参数调用 mlock 即可.
     * (Linux 分配内存到页(page)且每次只能锁定整页内存，被指定的区间涉及到的每个内存页都将被锁定)
     *
     * @param var1 内存地址起始指针
     * @param var2 内存区间大小
     * @return -1表示失败
     */
    int mlock(Pointer var1, NativeLong var2);

    /**
     * 解除被{{@link #mlock(Pointer, NativeLong)}}锁住的程序地址空间
     *
     * @param var1 内存地址起始指针
     * @param var2 内存区间大小
     * @return -1表示失败
     */
    int munlock(Pointer var1, NativeLong var2);

    /**
     * 系统调用-madvise(), 它允许程序告诉内核期望如何使用某些映射或内存共享区别, 然后内核可能会采纳它, 选择适当的预读和缓存技术, 加快程序运行性能.
     * 这个系统调用一般用在 mmap 映射,我们知道, linux系统存在<b>缺页中断</b>, 实际上它分为两类：
     * <pre>
     *     1.内存缺页中断, 这种的代表是 malloc, 利用malloc分配的内存只有在程序访问到的时候才会真正分配物理内存;
     *     2.硬盘缺页中断, 这种的代表是 mmap, 利用mmap映射后的只是逻辑地址, 当程序访问时, 内核才会将硬盘中的文件内容读进物理内存页中. 因此在使用mmap时, 访问内存中的数据延时会陡增
     * </pre>
     *
     * @param var1 指向内存区间开始的指针
     * @param var2 内存区间长度
     * @param var3 1.MADV_ACCESS_DEFAULT：此标志将指定范围的内核预期访问模式重置为缺省设置;
     *             2.MADV_ACCESS_LWP：此标志通知内核, 移近指定地址范围的下一个 LWP 就是将要访问此范围次数最多的 LWP. 内核将相应地为此范围和 LWP 分配内存和其他资源;
     *             3.MADV_ACCESS_MANY：此标志建议内核, 许多进程或 LWP 将在系统内随机访问指定的地址范围. 内核将相应地为此范围分配内存和其他资源.
     * @return
     * 1.EAGAIN: 指定地址范围（从 addr 到 addr+len）中的部分或所有映射均已锁定进行 I/O 操作;
     * 2.EINVAL: addr 参数的值不是sysconf(3C) 返回的页面大小的倍数, 指定地址范围的长度小于或等于零或者建议无效;
     * 3.EIO: 读写文件系统时发生 I/O 错误;
     * 4.ENOMEM: 指定地址范围中的地址不在进程的有效地址空间范围内, 或者指定地址范围中的地址指定了一个或多个未映射的页面;
     * 5.ESTALE: NFS 文件句柄过时;
     */
    int madvise(Pointer var1, NativeLong var2, int var3);

    Pointer memset(Pointer p, int v, long len);

    /**
     * 系统调用-mlockall, 会将程序的全部地址空间被锁定在物理内存中, 它接受一个参数：
     * - 如果指定 MCL_CURRENT, 则仅仅当前已分配的内存会被锁定, 之后分配的内存则不会;
     * - 如果指定 MCL_FUTURE, 则会锁定之后分配的所有内存;
     * - 如果指定 MCL_CURRENT|MCL_FUTURE, 将已经及将来分配的所有内存锁定在物理内存中
     *
     * @param flags 上述三种标志
     * @return -1表示失败
     */
    int mlockall(int flags);

    int msync(Pointer p, NativeLong length, int flags);
}
